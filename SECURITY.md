# Security policy

Please report vulnerabilities privately through
[GitHub Security Advisories](https://github.com/UDogg/DistKV/security/advisories/new)
rather than public issues. We aim to acknowledge reports within 72 hours.

In scope: authentication/TLS bypasses (e.g. reaching `/internal/raft/` or
`/v1/admin/` without a cluster certificate), remote crashes, and any way to
make the cluster violate linearizability or lose acknowledged writes.

Note that `--enable-fault-injection` and the demo supervisor are intended for
local demos and testing only and must not be exposed to untrusted networks.
