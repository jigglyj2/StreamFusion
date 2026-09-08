# Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
import hashlib
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import artifact


class ArtifactTest(unittest.TestCase):
    CPU = "vendor_id: GenuineIntel\ncpu family: 6\nmodel: 154\nflags: sse2 avx2 bmi1\n"

    def test_native_build_requires_union_of_all_build_processors(self):
        flags, identities = artifact.linux_cpu(self.CPU + "\n" + self.CPU.replace(" avx2", ""))
        self.assertEqual(flags, {"sse2", "avx2", "bmi1"})
        self.assertEqual(identities, {"260dec969707cba72a317a7e18dc56d00589add7c85246b707faa08087062985"})
        with self.assertRaises(ValueError):
            artifact.linux_cpu("processor: 0")

    def test_portable_cpu_levels_include_the_complete_inherited_baseline(self):
        v2, _ = artifact.requirements("x86-64-v2", "linux", "x86_64")
        v3, _ = artifact.requirements("x86-64-v3", "linux", "x86_64")
        v4, _ = artifact.requirements("x86-64-v4", "linux", "x86_64")
        self.assertLess(v2, v3)
        self.assertLess(v3, v4)
        self.assertLessEqual({"avx512cd", "cx16", "lahf_lm", "abm", "f16c", "xsave"}, v4)
        with self.assertRaises(ValueError):
            artifact.requirements("x86-64-v2", "linux", "aarch64")

    def test_mac_native_features_and_identity(self):
        flags, identities = artifact.darwin_cpu("hw.cputype: 16777228\nhw.cpusubtype: 2\nhw.optional.arm.FEAT_AES: 1\nhw.optional.arm.FEAT_SHA3: 0\n")
        self.assertEqual(flags, {"hw.optional.arm.feat_aes"})
        self.assertEqual(len(identities), 1)

    def test_manifest_is_written_only_after_a_release_build_and_binds_its_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            library = target / "release" / "library.so"
            library.parent.mkdir()
            calls = []

            def build(command, env, check):
                calls.append((command, env))
                self.assertTrue(check)
                self.assertFalse(library.with_name("library.so.properties").exists())
                library.write_bytes(b"compiled library")

            def stage(raw, os_name):
                packaged = raw.parent / "packaged" / raw.name
                packaged.parent.mkdir()
                shutil.copy2(raw, packaged)
                return packaged, {}

            argv = ["artifact.py", "--manifest", str(target / "Cargo.toml"), "--target-dir", directory,
                    "--library", "library.so", "--cpu", "x86-64-v3"]
            with patch("sys.argv", argv), patch.dict("os.environ", {}, clear=True), patch("platform.system", return_value="Linux"), patch("platform.machine", return_value="x86_64"), patch("subprocess.run", side_effect=build), patch("artifact.stage_library", side_effect=stage):
                artifact.main()
            command, env = calls[0]
            self.assertIn("--release", command)
            self.assertIn("--locked", command)
            self.assertIn("target-cpu=x86-64-v3", env["RUSTFLAGS"])
            self.assertIn("metadata=sf_cpu_", env["RUSTFLAGS"])
            metadata = (library.parent / "packaged" / "library.so.properties").read_text()
            self.assertIn("sha256=" + hashlib.sha256(b"compiled library").hexdigest(), metadata)
            self.assertIn("cpu-target=x86-64-v3", metadata)

    @unittest.skipUnless(platform.system() == "Linux" and shutil.which("cc") and shutil.which("objcopy"), "ELF toolchain required")
    def test_split_debug_artifact_preserves_execution_bytes_and_original_symbols(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "fixture.c"
            source.write_text("int fixture(int value) { return value + 1; }\n")
            library = root / "fixture.so"
            subprocess.run(["cc", "-g", "-O2", "-shared", "-fPIC", str(source), "-o", str(library)], check=True)
            original = library.read_bytes()
            packaged, symbols = artifact.stage_library(library, "linux")
            self.assertEqual(library.read_bytes(), original)
            debug = root / "symbols" / symbols["debug-file"]
            self.assertEqual(hashlib.sha256(debug.read_bytes()).hexdigest(), symbols["debug-sha256"])
            for label, path in [("original", library), ("packaged", packaged)]:
                subprocess.run(["objcopy", "-O", "binary", "--only-section=.text", str(path), str(root / (label + ".text"))], check=True)
            self.assertEqual((root / "original.text").read_bytes(), (root / "packaged.text").read_bytes())
            self.assertIn(symbols["debug-file"].encode(), packaged.read_bytes())
            self.assertLess(packaged.stat().st_size, library.stat().st_size)


if __name__ == "__main__":
    unittest.main()
