-module(edis_SUITE).
-compile(export_all).

all() ->
	[ping].

init_per_suite(Config) ->
	Port = erlang:open_port({spawn, "redis-server ./test/redis.conf"}, [binary, exit_status]), 
	Config.

end_per_suite(Config) ->
	Config.

ping(_Config) ->
	ok.