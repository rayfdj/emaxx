from pathlib import Path
import json,os,subprocess
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28');r=Path('/private/tmp/emaxx-runtime-frames-gsnnjxa_/emaxx')
env={'CARGO_BUILD_JOBS':'1','EMAXX_GC_VERIFY':'1','EMAXX_GNU_SOURCE_DIRECTORY':'/private/tmp/emaxx-runtime-gnu-20260928','EMAXX_DUMP_SOURCE_DIRECTORY':'/private/tmp/emaxx-runtime-gnu-20260928','EMACS_TEST_DIRECTORY':'/private/tmp/emaxx-runtime-gnu-20260928/test'}
filters=['native_funcall','native_apply','native_mapcar','native_maphash','native_runtime','function_cell','function_binding','function_alias','cyclic_function','uninterned','native_execution','native_preserves','window','frame','undo','record_index','pdumper','threads_retain_lexical_caller_roots','suspended_bytecode_retains_operand_and_unwind_roots']
commands=[['python3',str(p/'validate-frame-snapshot-v2.py'),'119',*filters,'--static'],['python3',str(p/'validate-frame-snapshot-v2.py'),'119',*filters,'--release'],['python3',str(p/'validate-frame-snapshot-v2.py'),'119','--ordinary']]
with (p/'source119-launch.json').open('x') as f:json.dump({'commands':commands,'cwd':str(r),'environment_overrides':env,'scope':'Combined committed native symbol resolver and existing window-index changes. Original native aliases/redefinition/errors, window/frame/undo/image and both suspended-root contracts; verifier enabled. Separate from the source115 main candidate. Selected validation is not full or performance certification.'},f,indent=2)
for cmd in commands:
 rc=subprocess.call(cmd,cwd=r,env={**os.environ,**env})
 if rc:raise SystemExit(rc)
