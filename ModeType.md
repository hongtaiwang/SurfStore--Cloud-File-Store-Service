Hash placement

For each new block that is added to the block store service, the assignment of that block to a block store server will be chosen by a hash function.

Nearest to client

This mode is to have the client measure the round-trip time to each of the BlockServer instances, and store all of its blocks on the “closest” BlockServer