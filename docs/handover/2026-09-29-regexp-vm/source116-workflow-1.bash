# Grant the installed sandbox executable its required namespace
# capability, following Ubuntu's application-profile mechanism.
# The host's general namespace restriction and GNU's BPF filters
# remain in force. This VM is discarded after the job.
sudo apparmor_parser -r compat/ci-bwrap.apparmor
bwrap --ro-bind / / -- /bin/true
mkdir -p target/frozen-ci/metadata
cp compat/ci-bwrap.apparmor target/frozen-ci/metadata/
