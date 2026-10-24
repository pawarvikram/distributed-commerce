# Chapter11 revision

Factory creates clients; template publishes; container polls; listener adapts; service owns business changes. Decode failure can precede listener invocation. Failed work must not return as success. Retry topics change ordering. DLT needs ownership and confirmed publication.

Retrieve: draw decode and business error paths; classify stock rejection versus database outage; design a DLT replay approval/check process appropriate to the application.

Checklist: [ ] configure [ ] classify [ ] integration test [ ] crash/replay [ ] assess. All embedded examples unexecuted.

