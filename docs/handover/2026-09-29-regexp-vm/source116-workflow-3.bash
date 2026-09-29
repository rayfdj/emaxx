mkdir -p target/frozen-ci/metadata
../emacs/src/emacs -Q --batch --eval \
  '(prin1 (list emacs-version emacs-repository-version system-type system-configuration-options system-configuration-features))' \
  > target/frozen-ci/metadata/oracle.txt
rustc --edition 2024 -O tools/generate_native_subrs.rs -o /tmp/generate-native-subrs
/tmp/generate-native-subrs ../emacs/src ../emacs/src/emacs /tmp/native-subrs.rs
rustfmt --edition 2024 /tmp/native-subrs.rs
cmp src/lisp/native_comp/generated_native_subrs_x86_64_unknown_linux_gnu.rs /tmp/native-subrs.rs
