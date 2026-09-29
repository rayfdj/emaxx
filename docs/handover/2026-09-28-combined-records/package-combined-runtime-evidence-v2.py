from pathlib import Path
import gzip,hashlib,json,subprocess
p=Path("/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28")
root=Path("/private/tmp/emaxx-runtime-reader-streams/emaxx")
out=root/"docs/handover/2026-09-28-combined-records"
assert not out.exists()
def read(name):return json.loads((p/name).read_text())
for stage in ["debug-selected","release-selected","release-broad","fmt","check","clippy","diff-check","ordinary-build","ordinary-image"]:
    result=read("source108-"+stage+"-result.json")
    assert result["exit_code"]==0 and result["source_unchanged"],stage
ordinary=read("source108-ordinary-combined-comparisons.json")
assert ordinary["all_match"] and ordinary["source_unchanged"] and len(ordinary["expected_inventory"])==len(ordinary["results"])==33
assert read("source108-dump-locale-comparisons.json")["all_match"]
assert read("source108-purecopy-tty-comparisons.json")["all_match"]
assert read("source108-eager-progn-gc-comparison.json")["matches"]
assert read("source105-release-selected-result.json")["exit_code"]==-11
assert read("source105-eager-progn-gc-emaxx-result.json")["exit_code"]==-11
assert read("source106-eager-progn-gc-emaxx-result.json")["exit_code"]==-11
assert read("source107-debug-selected-result.json")["exit_code"]!=0
assert read("source107-release-selected-result.json")["exit_code"]!=0
for before,after in [(103,105),(104,105),(105,108)]:
    assert not read(f"inventory-{before}-to-{after}-comparison.json")["removed"]
software=read("source108-manifest.json")
assert all(hashlib.sha256((root/name).read_bytes()).hexdigest()==sha for name,sha in software.items())
suffixes={".json",".log",".patch",".rs",".txt",".stdout",".stderr",".el",".expected",".report",".raw",".ips"}
files=set();snapshots={}
for n in [105,106,107,108]:
    tag=f"source{n}";base=read(tag+"-base.json")["commit"]
    tracked=set(subprocess.check_output(["git","ls-tree","-r","--name-only",base],cwd=root,text=True).splitlines())
    manifest=read(tag+"-manifest.json")
    additions={name:tag+"-"+Path(name).name for name in manifest if name not in tracked}
    for name,source in additions.items():assert hashlib.sha256((p/source).read_bytes()).hexdigest()==manifest[name]
    snapshots[tag]={"base_commit":base,"patch":tag+".patch.gz","software_manifest":tag+"-manifest.json","untracked_file_sources":additions}
    files.update(f for f in p.glob(tag+"*") if f.is_file() and f.suffix in suffixes and ("full-gate" not in f.name or n==105))
    for directory in [p/(tag+"-core-profiles"),p/(tag+"-core-native")]:
        if directory.exists():files.update(f for f in directory.rglob("*") if f.is_file() and f.suffix in suffixes)
files.update(f for f in (p/"full-gate-source105").rglob("*") if f.is_file() and f.suffix in suffixes)
files.add(p/"source108-full-gate-command.json")
files.add(p/"run-source105-full-gate.py")
files.add(p/"run-source108-full-gate.py")
for pattern in ["inventory-103-to-105-*","inventory-104-to-105-*","inventory-105-to-108-*"]:
    files.update(f for f in p.glob(pattern) if f.is_file())
for name in ["validate-frame-snapshot-v2.py","validate-combined-ordinary.py","validate-combined-ordinary-v2.py","probe-dump-locale-v2.py","probe-record-purecopy-tty.py","probe-record-purecopy-tty-v2.py","compare-record-purecopy-tty-v2.py","compare-source-inventories.py","package-combined-runtime-evidence.py","package-combined-runtime-evidence-v2.py","combined-record-text-conversion-resolution.json","dump-locale-quoting.el","purecopy-five-kinds-gc-tty.el","purecopy-sequence-snapshot-tty.el","purecopy-callback-purify-flag-tty.el","record-purecopy-callback.el","positioned-reader-streams-before.el","positioned-reader-streams-v2.el","callable-reader-consumption-before.el","reader-printer-destinations.el","profile-core-runtime-pilot.py","profile-source106-core-runtime-pilot.py","isolate-source105-ert.py","probe-eager-macroexpand-gc.py","eager-macroexpand-gc-source.el","launch-source107.py","launch-source108.py","run-source107-release-broad.py","run-source108-release-broad.py","run-source108-tty.py"]:
    files.add(p/name)
assert all(f.is_file() for f in files)
manifest={"format_version":1,"software_snapshot":108,"goal_complete":False,"source_snapshots":snapshots,"qualification":"Combined generic records, function cells, text-conversion buffer state; scoped eager-progn GC roots and regexp signature bookkeeping reduction. Source105 release ERT SIGSEGV, unchanged exact isolation failures, ordinary105/106SIGSEGV and107test-format assertion failures preserved. Source108selected checks pass. Prior parent checkpoint failures remain in their own evidence packages. Profiled pilots ran with concurrent validation and are not timing certification; workload bodies remain several times slower than GNU, allocation counters unavailable, frozen Darwin oracle absent. Source105 full gate was deliberately stopped after385+295passes and partialeval03 when superseded by108, which is separately running; interrupted and unexecuted stages are not passes. Full macOS/Linux/frozen/terminal validation and complete architecture/performance goal remain required.","evidence_files":{}}
out.mkdir(parents=True)
for path in sorted(files):
    raw=path.read_bytes();compress=path.suffix in {".log",".patch",".txt",".stdout",".stderr",".report",".raw",".ips"}
    name=str(path.relative_to(p))+(".gz" if compress else "");stored=gzip.compress(raw,mtime=0) if compress else raw
    dest=out/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(stored)
    manifest["evidence_files"][name]={"source_path":str(path),"sha256":hashlib.sha256(stored).hexdigest(),"raw_sha256":hashlib.sha256(raw).hexdigest(),"raw_bytes":len(raw)}
(out/"manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
for name,receipt in manifest["evidence_files"].items():
    stored=(out/name).read_bytes();raw=gzip.decompress(stored) if name.endswith(".gz") else stored
    assert hashlib.sha256(stored).hexdigest()==receipt["sha256"]
    assert hashlib.sha256(raw).hexdigest()==receipt["raw_sha256"] and len(raw)==receipt["raw_bytes"]
print("Verified",len(manifest["evidence_files"]),"portable combined-runtime receipts")
