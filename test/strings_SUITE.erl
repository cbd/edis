-module(strings_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
	[set].
%% 	append,decr,decrby,get,getbit,
%% 	 getrange,getset,incr,incrby,mget,
%% 	 mset,msetnx,setbit,setex,
%% 	 setnx,setrange,strlen].

init_per_testcase(_TestCase,Config) ->
	{ok,Client} = connect_erldis(10),
	NewConfig = lists:keystore(client,1,Config,{client,Client}),
	NewConfig.

connect_erldis(0) -> {error,{socket_error,econnrefused}};
connect_erldis(Times) ->
	timer:sleep(2000),
	case erldis:connect(localhost,16380) of
		{ok,Client} -> {ok,Client};
		_ -> connect_erldis(Times - 1)
	end.

set(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	ok = erldis_client:sr_scall(Client, [<<"set">>,<<"name">>,<<"inaka labs">>]),
	{error,<<"ERR wrong number of arguments for 'set' command">>}  = erldis_client:sr_scall(Client, [<<"set">>,<<"name">>,<<"inaka">>,<<"labs">>]),
	{error,<<"ERR wrong number of arguments for 'set' command">>}  = erldis_client:sr_scall(Client, [<<"set">>,<<"name">>]),
	{error,<<"ERR wrong number of arguments for 'set' command">>}  = erldis_client:sr_scall(Client, [<<"set">>]).


	