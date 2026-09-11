// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn spill_assignments_are_versioned_validated_and_lazy() {
    let directory = tempfile::tempdir().unwrap();
    for plan in plans() {
        let broker = Arc::new(TestBroker::new(LIMIT));
        let mut context = context(&plan, &broker);
        let baseline = broker.reserved();
        for (version, directories) in [
            (1, vec![directory.path().to_str().unwrap().to_owned()]),
            (3, vec![directory.path().to_str().unwrap().to_owned()]),
            (4, vec![]),
            (4, vec!["relative".to_owned()]),
            (
                4,
                vec![directory
                    .path()
                    .join("missing")
                    .to_str()
                    .unwrap()
                    .to_owned()],
            ),
        ] {
            let mut options = resources();
            options.protocol_version = version;
            options.spill_directories = directories;
            assert!(install(&mut context, &broker, &options.encode_to_vec()).is_err());
            assert!(context.persistent.is_empty());
            assert!(context.state_resources.is_none());
            assert_eq!(broker.reserved(), baseline);
        }
        let mut valid = resources();
        valid.protocol_version = 4;
        valid.spill_directories = vec![directory.path().to_str().unwrap().to_owned()];
        install(&mut context, &broker, &valid.encode_to_vec()).unwrap();
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}
