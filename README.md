An *Erlang* version of [Redis](http://redis.io). Still in its very early stages.

## Usage
Just run `$ make run` and open connections with your favourite redis client.

## Differences with Redis
### Different Behaviour
* _SAVE_, _BGSAVE_ and _LASTSAVE_ are database dependent. The original Redis saves all databases at once, edis saves just the one you _SELECT_'ed.
* _INFO_ provides much less information and no statistics (so, _CONFIG RESETSTAT_ does nothing at all)

### Missing Features
* Dynamic node configuration (i.e. the _SLAVEOF_ command is not implemented)

### Unsupported Commands
_SYNC_, _SLOWLOG_, _SLAVEOF_, _DEBUG *_