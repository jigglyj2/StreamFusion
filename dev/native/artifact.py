#!/usr/bin/env python3
# Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
"""Build release libraries and bind their CPU requirements to their exact bytes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess


# x86-64 psABI levels, expressed as Linux /proc/cpuinfo feature names. Linux reports
# AVX only when the kernel has enabled the required extended register state.
# https://gitlab.com/x86-psABIs/x86-64-ABI
X86_LEVELS = {
    "x86-64-v2": set("cmov cx8 fpu fxsr mmx syscall sse sse2 cx16 lahf_lm popcnt pni sse4_1 sse4_2 ssse3".split()),
    "x86-64-v3": set("avx avx2 bmi1 bmi2 f16c fma abm movbe xsave".split()),
    "x86-64-v4": set("avx512f avx512bw avx512cd avx512dq avx512vl".split()),
}


def linux_cpu(text):
    features, identities = set(), set()
    identity_keys = {"vendor_id", "cpu family", "model", "cpu implementer", "cpu architecture", "cpu part"}
    for block in text.lower().strip().split("\n\n"):
        values = dict(line.split(":", 1) for line in block.splitlines() if ":" in line)
        values = {key.strip(): value.strip() for key, value in values.items()}
        if "flags" not in values and "features" not in values:
            continue
        features.update(values.get("flags", values.get("features", "")).split())
        identity = "\n".join(f"{key}={values[key]}" for key in sorted(identity_keys & values.keys()))
        if not identity:
            raise ValueError("Cannot identify native build CPU")
        identities.add(hashlib.sha256(identity.encode()).hexdigest())
    if not features or not identities:
        raise ValueError("Cannot determine native CPU requirements")
    return features, identities


def darwin_cpu(text):
    values = dict(line.split(":", 1) for line in text.lower().splitlines() if ":" in line)
    values = {key.strip(): value.strip() for key, value in values.items()}
    features = {key for key, value in values.items() if key.startswith("hw.optional.") and value == "1"}
    for key in ("machdep.cpu.features", "machdep.cpu.extfeatures", "machdep.cpu.leaf7_features"):
        features.update(values.get(key, "").split())
    identity_keys = {"machdep.cpu.brand_string", "hw.cputype", "hw.cpusubtype"}
    identity = "\n".join(f"{key}={values[key]}" for key in sorted(identity_keys & values.keys()))
    if not features or not identity:
        raise ValueError("Cannot determine native macOS CPU requirements")
    return features, {hashlib.sha256(identity.encode()).hexdigest()}


def requirements(cpu, os_name, arch):
    if cpu == "native":
        if os_name == "linux":
            features, identities = linux_cpu(Path("/proc/cpuinfo").read_text())
        elif os_name == "darwin":
            features, identities = darwin_cpu(subprocess.check_output(["sysctl", "-a"], text=True))
        else:
            raise ValueError(f"Unsupported native artifact OS: {os_name}")
        return features, identities
    if os_name != "linux" or arch != "x86_64" or cpu not in X86_LEVELS:
        raise ValueError(f"Unsupported portable CPU target: {os_name}/{arch}/{cpu}")
    features = set()
    for level, added in X86_LEVELS.items():
        features.update(added)
        if level == cpu:
            return features, set()


def stage_library(library, os_name):
    """Keep profiling data separate from the bytes loaded on the execution path."""
    packaged = library.parent / "packaged" / library.name
    packaged.parent.mkdir(parents=True, exist_ok=True)
    symbols = {}
    if os_name == "linux":
        objcopy = shutil.which("llvm-objcopy") or shutil.which("objcopy")
        if objcopy is None:
            raise ValueError("ELF artifact builds require llvm-objcopy or objcopy")
        digest = hashlib.sha256(library.read_bytes()).hexdigest()
        debug = library.parent / "symbols" / f"{library.name}.{digest}.debug"
        debug.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run([objcopy, "--only-keep-debug", str(library), str(debug)], check=True)
        subprocess.run([objcopy, "--strip-debug", str(library), str(packaged)], check=True)
        subprocess.run([objcopy, f"--add-gnu-debuglink={debug}", str(packaged)], check=True)
        symbols = {"debug-file": debug.name, "debug-sha256": hashlib.sha256(debug.read_bytes()).hexdigest()}
    else:
        # Mach-O keeps its profiling information in the native artifact until a dSYM
        # publication contract is implemented. Never apply ELF stripping to Mach-O.
        shutil.copy2(library, packaged)
    return packaged, symbols


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--library", required=True)
    parser.add_argument("--cpu", default="native", choices=["native", *X86_LEVELS])
    args = parser.parse_args()
    os_name = platform.system().lower()
    arch = {"amd64": "x86_64", "arm64": "aarch64"}.get(platform.machine().lower(), platform.machine().lower())
    features, identities = requirements(args.cpu, os_name, arch)
    config = {
        "format": "1", "os": os_name, "arch": arch, "cpu-target": args.cpu,
        "required-features": " ".join(sorted(features)),
        "cpu-identities": " ".join(sorted(identities)),
    }
    # Native Cargo caches must not be reused after moving between CPU models. Make the
    # CPU contract part of every crate's fingerprint, including the C++ build script.
    fingerprint = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:16]
    env = os.environ.copy()
    for key in env:
        if key in {"CARGO_ENCODED_RUSTFLAGS", "RUSTFLAGS", "CARGO_BUILD_TARGET", "ROCKSDB_LIB_DIR", "SNAPPY_LIB_DIR"} or key.startswith(("CXXFLAGS", "CFLAGS", "CARGO_TARGET_")):
            raise ValueError(f"Release artifact builds own their compiler target; remove {key}")
    env["RUSTFLAGS"] = f"-C target-cpu={args.cpu} -C force-frame-pointers=yes -C debuginfo=1 -C metadata=sf_cpu_{fingerprint}"
    env["CARGO_TARGET_DIR"] = str(args.target_dir.resolve())
    env.setdefault("CARGO_BUILD_JOBS", "2")
    # Cargo's default release profile otherwise strips DWARF at the final link even
    # when dependencies were compiled with -C debuginfo=1. Retain it on the root
    # library without changing dependency optimization or rebuilding their code.
    subprocess.run(["cargo", "rustc", "--release", "--locked", "--lib", "--manifest-path", str(args.manifest.resolve()), "--", "-C", "strip=none"], env=env, check=True)
    raw_library = args.target_dir / "release" / args.library
    library, symbols = stage_library(raw_library, os_name)
    config.update(symbols)
    config["sha256"] = hashlib.sha256(library.read_bytes()).hexdigest()
    config["rustflags"] = env["RUSTFLAGS"]
    config["link-rustflags"] = "-C strip=none"
    manifest = library.with_name(library.name + ".properties")
    manifest.write_text("".join(f"{key}={value}\n" for key, value in config.items()))
    # A direct Cargo output is not the packaged artifact. Remove obsolete manifests
    # from the former location so scripts cannot accidentally copy the wrong pair.
    raw_library.with_name(raw_library.name + ".properties").unlink(missing_ok=True)
    print(f"Verified release artifact: {library} ({args.cpu}, {fingerprint})", flush=True)


if __name__ == "__main__":
    main()
