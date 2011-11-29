%% @hidden
-module(sets_SUITE).
-compile(export_all).

all() ->
	[basic].

init_per_testcase(_TestCase,Config) ->
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

%% SADD, SCARD, SISMEMBER, SMEMBERS basics
basic(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	%% Regular set
	true = erldis_client:sr_scall(Client,[<<"sadd">>,<<"myset">>,<<"foo">>]),
	true = erldis_client:sr_scall(Client,[<<"sadd">>,<<"myset">>,<<"bar">>]),
	false = erldis_client:sr_scall(Client,[<<"sadd">>,<<"myset">>,<<"bar">>]),
	2 = erldis_client:sr_scall(Client,[<<"scard">>,<<"myset">>]),
	true = erldis_client:sr_scall(Client,[<<"sismember">>,<<"myset">>,<<"foo">>]),
	true = erldis_client:sr_scall(Client,[<<"sismember">>,<<"myset">>,<<"bar">>]),
	false = erldis_client:sr_scall(Client,[<<"sismember">>,<<"myset">>,<<"buzz">>]),
	[<<"bar">>,<<"foo">>] = erldis_client:scall(Client,[<<"smembers">>,<<"myset">>]),
	
	
	%% Intset
	true = erldis_client:sr_scall(Client,[<<"del">>,<<"myset">>]),
	true = erldis_client:sr_scall(Client,[<<"sadd">>,<<"myset">>,17]),
	true = erldis_client:sr_scall(Client,[<<"sadd">>,<<"myset">>,16]),
	false = erldis_client:sr_scall(Client,[<<"sadd">>,<<"myset">>,16]),
	2 = erldis_client:sr_scall(Client,[<<"scard">>,<<"myset">>]),
	true = erldis_client:sr_scall(Client,[<<"sismember">>,<<"myset">>,16]),
	true = erldis_client:sr_scall(Client,[<<"sismember">>,<<"myset">>,17]),
	false = erldis_client:sr_scall(Client,[<<"sismember">>,<<"myset">>,18]),
	[<<"16">>,<<"17">>] = erldis_client:scall(Client,[<<"smembers">>,<<"myset">>]),
	ok.