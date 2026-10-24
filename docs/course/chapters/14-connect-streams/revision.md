# Chapter14 revision

Connect: worker/connector/task/converter/offset. Streams: topology/task/key/serde/state/changelog. Stream=events; table=current keyed view. Event time differs from processing time. Repartition redistributes by key. Changelog restores state. Sink effects need destination-specific guarantees.

Retrieve: solve event counts versus current counts; draw state restore; explain late events and join windows.

Checklist: [ ] concepts [ ] topology test [ ] real restart [ ] connector failure [ ] assessment. Execution pending.

