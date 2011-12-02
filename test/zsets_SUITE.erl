%% @hidden
-module(zsets_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [basic].

init_per_suite(Config) ->
	{ok,Client} = connect_erldis(10),
	NewConfig = lists:keystore(client,1,Config,{client,Client}),
	NewConfig.

init_per_testcase(_TestCase,Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	erldis_client:sr_scall(Client,[<<"flushdb">>]),
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

basic(Config) ->
	%% basic ZADD and score update
	{client,Client} = lists:keyfind(client, 1, Config),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,10,<<"x">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,20,<<"y">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,30,<<"z">>]),
	[<<"x">>,<<"y">>,<<"z">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,0,-1]),
	false = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,1,<<"y">>]),
	[<<"y">>,<<"x">>,<<"z">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,0,-1]),
	%% element can't be set to NaN with ZADD
	{error,<<"ERR value is not a double">>} = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,<<"nan">>,<<"x">>]),
	ok.
