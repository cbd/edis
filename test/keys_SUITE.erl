%% @hidden
-module(keys_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> 
     [exists_del, object_idletime].
 	
init_per_suite(Config) ->
	{ok,Client} = connect_erldis(10),
	NewConfig = lists:keystore(client,1,Config,{client,Client}),
	NewConfig.

init_per_testcase(_TestCase,Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	erldis_client:sr_scall(Client,[<<"flushdb">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"string">>,<<"I'm not a list">>]),
	Config.

end_per_suite(_Config) ->
		ok.

connect_erldis(0) -> {error,{socket_error,econnrefused}};
connect_erldis(Times) ->
	timer:sleep(2000),
	case erldis:connect(localhost,16380) of
		{ok,Client} -> {ok,Client};
		_ -> connect_erldis(Times - 1)
	end.


exists_del(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),

	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key1">>,<<"hello">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key2">>,<<"world">>]),
	true = erldis_client:sr_scall(Client,[<<"exists">>,<<"key1">>]),

	2 = erldis_client:sr_scall(Client,[<<"del">>,<<"key1">>, <<"key2">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"key1">>]).


object_idletime(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),

	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key1">>,<<"hello">>]),
	timer:sleep(2000),
	2 = erldis_client:sr_scall(Client,[<<"object">>,<<"idletime">>,<<"key1">>]).






