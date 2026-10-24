# Chapter26 revision

Safety: forbidden outcomes never occur. Liveness: useful work progresses under assumptions. Linearizability respects real-time operation order; serializability concerns transaction equivalence. CAP concerns partitions; PACELC includes healthy-operation latency. Quorum intersection is only one proof ingredient. Fencing rejects obsolete ownership at the resource.

Practice: draw a lost-response timeline; calculate nested retry multiplication; solve regional stock allocation; explain lease expiry after a pause.

Checklist: [ ] models [ ] assumptions [ ] fencing simulation [ ] retry budget [ ] failure reasoning [ ] assessment. No distributed protocol execution claimed.

