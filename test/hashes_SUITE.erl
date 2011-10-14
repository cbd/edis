-module(hashes_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
	[hset_hlen,hset_hget].

init_per_testcase(TestCase,Config) ->
	ct:print("*** Init ~p Test ***",[TestCase]),
	{ok,Client} = connect_erldis(10),
    erldis_client:sr_scall(Client,[<<"flushdb">>]),
	
	NewConfig = lists:keystore(client,1,Config,{client,Client}),
	NewConfig.

connect_erldis(0) -> {error,{socket_error,econnrefused}};
connect_erldis(Times) ->
	timer:sleep(2000),
	case erldis:connect(localhost,16380) of
		{ok,Client} -> {ok,Client};
		_ -> connect_erldis(Times - 1)
	end.

hset_hlen(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
		
	%% Small hash creation
	[true = erldis_client:sr_scall(Client,[<<"hset">>,<<"smallhash">>,edis_util:random_binary(),edis_util:random_binary()])
	|| _ <- lists:seq(1,8)],
	8 = erldis_client:sr_scall(Client,[<<"hlen">>,<<"smallhash">>]),
	
	%% Big hash creation
	[true = erldis_client:sr_scall(Client,[<<"hset">>,<<"bighash">>,edis_util:random_binary(),edis_util:random_binary()])
	|| _ <- lists:seq(1,1024)],
	1024 = erldis_client:sr_scall(Client,[<<"hlen">>,<<"bighash">>]),
	
	{error,<<"ERR wrong number of arguments for 'HSET' command">>} = erldis_client:sr_scall(Client,[<<"hset">>,<<"smallhash">>,<<"field">>]),
	{error,<<"ERR wrong number of arguments for 'HSET' command">>} = erldis_client:sr_scall(Client,[<<"hset">>,<<"smallhash">>,<<"field">>,<<"value">>,<<"field2">>]),
	{error,<<"ERR wrong number of arguments for 'HLEN' command">>} = erldis_client:sr_scall(Client,[<<"hlen">>,<<"smallhash">>,<<"field">>]),
	{error,<<"ERR wrong number of arguments for 'HLEN' command">>} = erldis_client:sr_scall(Client,[<<"hlen">>]).

hset_hget(Config) -> 
	{client,Client} = lists:keyfind(client, 1, Config),
	
	[true = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,edis_util:integer_to_binary(E),list_to_binary("value "++integer_to_list(E))])
	|| E <- lists:seq(1,8)],
	
	<<"value 1">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"1">>]),
	<<"value 5">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"5">>]),
	<<"value 8">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"8">>]),
	nil = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"9">>]),
	nil = erldis_client:sr_scall(Client,[<<"hget">>,<<"nonexistinghash">>,<<"3">>]),
	
	%% Set Already existing field
	false = erldis_client:sr_scall(Client,[<<"hset">>,<<"myhash">>,<<"5">>,<<"foo">>]),
	<<"foo">> = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"5">>]),
	
	{error,<<"ERR wrong number of arguments for 'HGET' command">>} = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>]),
	{error,<<"ERR wrong number of arguments for 'HGET' command">>} = erldis_client:sr_scall(Client,[<<"hget">>,<<"myhash">>,<<"8">>,<<"9">>]).

