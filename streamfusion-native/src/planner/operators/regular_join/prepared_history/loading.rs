// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

/// Read every required backend value before returning. Payload requests span key boundaries;
/// neither replaying an input row nor advancing its output cursor calls the state backend.
pub(in super::super) fn load(
    state: &dyn KeyedState,
    keys: Vec<StateKey>,
    side: usize,
    encoded: &Rows,
    indices: &[usize],
    kinds: &Int8Array,
    manager: &Arc<DiskManager>,
    owner: &HostMemoryReservation,
    reads: &mut u64,
) -> Result<(Vec<StagedState>, Prepared)> {
    if kinds.null_count() != 0 || kinds.values().iter().any(|kind| !(0..=3).contains(kind)) {
        return Err(DataFusionError::Execution(
            "invalid regular join RowKinds".into(),
        ));
    }
    let mut pending: HashMap<usize, HashMap<&[u8], VecDeque<usize>, RandomState>, RandomState> =
        HashMap::with_hasher(RandomState::new());
    for row in 0..indices.len() {
        if matches!(kinds.value(row), DELETE | UPDATE_BEFORE) {
            pending
                .entry(indices[row])
                .or_default()
                .entry(encoded.row(row).data())
                .or_default()
                .push_back(row);
        }
    }
    let mut retracted_history = vec![false; indices.len()];
    let mut staged = Vec::with_capacity(keys.len());
    let mut groups = Vec::with_capacity(keys.len());
    let mut requests = Vec::with_capacity(keys.len() * 2);
    let mut memory = owner.sibling("regular join prepared history directories");
    let mut retained = 0usize;
    let mut writer = Writer::new(manager, owner)?;
    let mut offset = 0;
    while offset < keys.len() {
        writer.prepare_read()?;
        let mut count = (keys.len() - offset).min(64);
        loop {
            let mut transport = owner.sibling("regular join spill manifest transport");
            match transport.resize(
                keys[offset..offset + count]
                    .iter()
                    .fold(count * 256, |bytes, key| {
                        bytes.saturating_add(key.key.len())
                    }),
            ) {
                Err(DataFusionError::ResourcesExhausted(_)) if count > 1 => {
                    count = (count / 2).max(1);
                    continue;
                }
                result => result?,
            }
            let manifest_keys = keys[offset..offset + count]
                .iter()
                .map(manifest_key)
                .collect::<Vec<_>>();
            let refs = refs(&manifest_keys);
            *reads = reads.saturating_add(1);
            let values = match state.get_batch(&refs, owner) {
                Err(DataFusionError::ResourcesExhausted(_)) if count > 1 => {
                    count = (count / 2).max(1);
                    continue;
                }
                result => result?,
            };
            let mut decode = owner.sibling("regular join spill manifest decode");
            let bytes = values.iter().flatten().try_fold(0usize, |bytes, value| {
                Ok::<_, DataFusionError>(bytes.saturating_add(manifest_workspace(value)?))
            })?;
            match decode.resize(bytes) {
                Err(DataFusionError::ResourcesExhausted(_)) if count > 1 => {
                    count = (count / 2).max(1);
                    continue;
                }
                result => result?,
            }
            for (position, value) in values.iter().enumerate() {
                let index = offset + position;
                let key = keys[index].clone();
                let Some(value) = value else {
                    staged.push(StagedState {
                        key,
                        value: JoinState::default(),
                        original: JoinState::default(),
                        original_layout: Layout::Compact,
                        unloaded: None,
                        touched: false,
                    });
                    groups.push(Group::default());
                    continue;
                };
                let manifest = decode_manifest(value)?;

                let directory_bytes = if manifest.layout == Layout::Rows {
                    manifest_workspace(value)?.saturating_add(512)
                } else {
                    manifest
                        .pages
                        .iter()
                        .map(EntryIds::len)
                        .sum::<usize>()
                        .saturating_mul(128)
                        .saturating_add(1024)
                };
                retained = retained.saturating_add(directory_bytes);
                admit(&mut memory, retained)?;
                let metadata = JoinState {
                    next_row_id: manifest.next_row_id,
                    left_matchable: manifest.matchable[0],
                    right_matchable: manifest.matchable[1],
                    ..Default::default()
                };
                let mut group = Group::default();
                let mut unloaded;
                if manifest.layout == Layout::Rows {
                    for input in 0..2 {
                        if input != side || pending.contains_key(&index) {
                            requests.push((
                                index,
                                input,
                                manifest.layout,
                                manifest.pages[input].clone(),
                            ));
                        }
                    }
                    unloaded = UnloadedRows::new(manifest.pages);
                } else {
                    let capacities = manifest.pages.each_ref().map(|ids| ids.len().max(1));
                    unloaded = UnloadedRows::new(capacities.map(EntryIds::with_bitmap_capacity));
                    group.legacy = Some(Legacy {
                        layout: manifest.layout,
                        pages: manifest.pages.clone(),
                    });
                    if let Some(inline) = manifest.inline {
                        for (input, rows) in inline.into_iter().enumerate() {
                            for row in &rows {
                                unloaded.ids[input].append(row.id);
                                unloaded.original_ids[input].append(row.id);
                                match_retraction(
                                    index,
                                    input,
                                    side,
                                    row,
                                    &mut pending,
                                    &mut retracted_history,
                                    &mut unloaded,
                                );
                            }
                            if !rows.is_empty() {
                                writer.start_group()?;
                                write_pages(&mut writer, &rows)?;
                                group.ranges[input] = Some(writer.finish_group()?);
                            }
                        }
                    } else {
                        for (input, ids) in manifest.pages.into_iter().enumerate() {
                            requests.push((index, input, manifest.layout, ids));
                        }
                    }
                }

                staged.push(StagedState {
                    key,
                    value: metadata.clone(),
                    original: metadata,
                    original_layout: manifest.layout,
                    unloaded: Some(unloaded),
                    touched: false,
                });
                groups.push(group);
            }
            offset += count;
            break;
        }
    }
    let mut locations = requests
        .into_iter()
        .flat_map(|(index, input, layout, ids)| {
            ids.into_iter().map(move |id| (index, input, id, layout))
        });
    let mut active: Option<(usize, usize)> = None;
    loop {
        let locations = locations.by_ref().take(128).collect::<Vec<_>>();
        if locations.is_empty() {
            break;
        }
        let mut start = 0;
        while start < locations.len() {
            writer.prepare_read()?;
            let mut count = locations.len() - start;
            loop {
                let chunk = &locations[start..start + count];
                let mut transport = owner.sibling("regular join spill payload transport");
                match transport.resize(chunk.iter().fold(count * 256, |bytes, (index, _, _, _)| {
                    bytes.saturating_add(staged[*index].key.key.len())
                })) {
                    Err(DataFusionError::ResourcesExhausted(_)) if count > 1 => {
                        count = (count / 2).max(1);
                        continue;
                    }
                    result => result?,
                }
                let keys = chunk
                    .iter()
                    .map(|&(index, input, id, layout)| {
                        entry_key(&staged[index].key, input, id, layout)
                    })
                    .collect::<Vec<_>>();
                let refs = refs(&keys);
                *reads = reads.saturating_add(1);
                let values = match state.get_batch(&refs, owner) {
                    Err(DataFusionError::ResourcesExhausted(_)) if count > 1 => {
                        count = (count / 2).max(1);
                        continue;
                    }
                    result => result?,
                };
                let mut decode = owner.sibling("regular join spill payload decode");
                let bytes = values.iter().flatten().try_fold(0usize, |bytes, value| {
                    Ok::<_, DataFusionError>(bytes.saturating_add(decode_workspace(value)?))
                })?;
                match decode.resize(bytes) {
                    Err(DataFusionError::ResourcesExhausted(_)) if count > 1 => {
                        count = (count / 2).max(1);
                        continue;
                    }
                    result => result?,
                }
                let mut at = 0;
                while at < chunk.len() {
                    let (index, input, _, layout) = chunk[at];
                    let mut rows = Vec::new();
                    while at < chunk.len() && (chunk[at].0, chunk[at].1) == (index, input) {
                        let bytes = values[at].as_ref().ok_or_else(|| {
                            DataFusionError::Execution(
                                "join directory references missing history".into(),
                            )
                        })?;
                        rows.extend(decode_entry(
                            bytes,
                            chunk[at].2,
                            staged[index].value.next_row_id[input],
                            layout,
                        )?);
                        at += 1;
                    }
                    let unloaded = staged[index].unloaded.as_mut().unwrap();
                    for row in &rows {
                        if layout != Layout::Rows {
                            unloaded.ids[input].append(row.id);
                            unloaded.original_ids[input].append(row.id);
                        }
                        match_retraction(
                            index,
                            input,
                            side,
                            row,
                            &mut pending,
                            &mut retracted_history,
                            unloaded,
                        );
                    }
                    if input != side || layout != Layout::Rows {
                        if active != Some((index, input)) {
                            if let Some((previous, input)) = active.take() {
                                groups[previous].ranges[input] = Some(writer.finish_group()?);
                            }
                            writer.start_group()?;
                            active = Some((index, input));
                        }
                        write_pages(&mut writer, &rows)?;
                    }
                }
                start += count;
                break;
            }
        }
    }
    if let Some((index, input)) = active {
        groups[index].ranges[input] = Some(writer.finish_group()?);
    }
    Ok((
        staged,
        Prepared {
            history: writer.finish()?,
            groups,
            retracted_history,
            _memory: memory,
        },
    ))
}

fn refs(keys: &[StateKey]) -> Vec<StateKeyRef<'_>> {
    keys.iter()
        .map(|key| StateKeyRef {
            key_group: key.key_group,
            key: &key.key,
        })
        .collect()
}

fn admit(memory: &mut HostMemoryReservation, bytes: usize) -> Result<()> {
    if bytes <= memory.size() {
        return Ok(());
    }
    memory.resize(bytes.saturating_add(65535) / 65536 * 65536)
}

fn match_retraction(
    index: usize,
    input: usize,
    side: usize,
    row: &StoredRow,
    pending: &mut HashMap<usize, HashMap<&[u8], VecDeque<usize>, RandomState>, RandomState>,
    retracted: &mut [bool],
    unloaded: &mut UnloadedRows,
) {
    if input == side {
        if let Some(row_index) = pending
            .get_mut(&index)
            .and_then(|rows| rows.get_mut(row.row.as_ref()))
            .and_then(VecDeque::pop_front)
        {
            assert!(unloaded.ids[input].remove(row.id));
            retracted[row_index] = true;
        }
    }
}

fn write_pages(writer: &mut Writer, rows: &[StoredRow]) -> Result<()> {
    let mut start = 0;
    while start < rows.len() {
        let mut count = 0;
        let mut bytes = 0usize;
        while start + count < rows.len()
            && count < 128
            && (count == 0 || bytes + rows[start + count].row.len() <= 64 << 10)
        {
            bytes += rows[start + count].row.len();
            count += 1;
        }
        loop {
            match writer.write(&rows[start..start + count]) {
                Err(DataFusionError::ResourcesExhausted(_)) if count > 1 => {
                    count = (count / 2).max(1)
                }
                result => {
                    result?;
                    break;
                }
            }
        }
        start += count;
    }
    Ok(())
}
