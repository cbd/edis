%% Author: Joachim Nilsson joachim@inakanetworks.com

%% SCOPE: 
%% OBJECT subtypes "refcount" and "encoding" are not tested, only "idletime"
%% SORT has not been covered, very complicated


-module(keys_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

all() -> 
     [exists_del, object_idletime, expire_ttl, expireat, persist, rename, renamenx, type, move, pattern, randomkey].
 	
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

generate_random(Amount, Client) -> generate_random(Amount,[], Client).
generate_random(0, Acc, _Client) -> Acc;
generate_random(Amount, Acc, Client) -> 
	NewAcc = [erldis_client:sr_scall(Client,[<<"randomkey">>]) | Acc],
	generate_random(Amount-1, NewAcc, Client).





%%%%%%%%%%%
%% TESTS %%
%%%%%%%%%%%

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


expire_ttl(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),

	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key1">>,<<"hello">>]),
	true = erldis_client:sr_scall(Client,[<<"del">>,<<"key1">>,<<"hello">>]),
	-1 = erldis_client:sr_scall(Client,[<<"ttl">>,<<"key1">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key1">>,<<"hello">>]),
	-1 = erldis_client:sr_scall(Client,[<<"ttl">>,<<"key1">>]),
	true = erldis_client:sr_scall(Client,[<<"expire">>,<<"key1">>,<<"3">>]),
	timer:sleep(1000),
	2 = erldis_client:sr_scall(Client,[<<"ttl">>,<<"key1">>]),
	timer:sleep(2000),
	false = erldis_client:sr_scall(Client,[<<"ttl">>,<<"key1">>]).


expireat(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),

	%% Retrieving Unix timestamp
	Epoch = calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
	Timestamp = calendar:datetime_to_gregorian_seconds(calendar:universal_time())-Epoch,
	WaitThree = Timestamp + 3,

	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key1">>,<<"hello">>]),
	true = erldis_client:sr_scall(Client,[<<"expireat">>,<<"key1">>,WaitThree]),
	timer:sleep(1000),
	2 = erldis_client:sr_scall(Client,[<<"ttl">>,<<"key1">>]),
	timer:sleep(2000),
	false = erldis_client:sr_scall(Client,[<<"ttl">>,<<"key1">>]).



persist(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),

	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key1">>,<<"hello">>]),
	true = erldis_client:sr_scall(Client,[<<"expire">>,<<"key1">>,<<"3">>]),
	timer:sleep(1000),
	2 = erldis_client:sr_scall(Client,[<<"ttl">>,<<"key1">>]),
	true = erldis_client:sr_scall(Client,[<<"persist">>,<<"key1">>]),
	timer:sleep(2500),
	true = erldis_client:sr_scall(Client,[<<"exists">>,<<"key1">>]).

rename(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),

	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key1">>,<<"hello">>]),
	OldKey = erldis_client:sr_scall(Client,[<<"get">>,<<"key1">>]),
	ok = erldis_client:sr_scall(Client,[<<"rename">>,<<"key1">>,<<"key2">>]),
	OldKey = erldis_client:sr_scall(Client,[<<"get">>,<<"key2">>]).

renamenx(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),

	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key1">>,<<"hello">>]),
	OldKey = erldis_client:sr_scall(Client,[<<"get">>,<<"key1">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key2">>,<<"world">>]),
	false = erldis_client:sr_scall(Client,[<<"renamenx">>,<<"key1">>,<<"key2">>]),

	erldis_client:sr_scall(Client,[<<"del">>,<<"key2">>]),
	true = erldis_client:sr_scall(Client,[<<"renamenx">>,<<"key1">>,<<"key2">>]),
	OldKey = erldis_client:sr_scall(Client,[<<"get">>,<<"key2">>]).

type(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),

	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key1">>,<<"hello">>]),
	true = erldis_client:sr_scall(Client,[<<"lpush">>,<<"key2">>,<<"hello">>]),
	true = erldis_client:sr_scall(Client,[<<"sadd">>,<<"key3">>,<<"hello">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"key4">>,<<"1">>, <<"uno">>]),
	true = erldis_client:sr_scall(Client,[<<"hset">>,<<"key5">>,<<"age">>, <<"5">>]),

	<<"string">> = erldis_client:sr_scall(Client,[<<"type">>,<<"key1">>]),
	erldis_client:sr_scall(Client,[<<"del">>,<<"key1">>]),
	<<"none">> = erldis_client:sr_scall(Client,[<<"type">>,<<"key1">>]),
	<<"list">> = erldis_client:sr_scall(Client,[<<"type">>,<<"key2">>]),
	<<"set">> = erldis_client:sr_scall(Client,[<<"type">>,<<"key3">>]),
	<<"zset">> = erldis_client:sr_scall(Client,[<<"type">>,<<"key4">>]),
	<<"hash">> = erldis_client:sr_scall(Client,[<<"type">>,<<"key5">>]).

move(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),

	ok = erldis_client:sr_scall(Client,[<<"select">>,<<"2">>]),
	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	ok = erldis_client:sr_scall(Client,[<<"select">>,<<"1">>]),
	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"key">>,<<"hello">>]),
	OldKey = erldis_client:sr_scall(Client,[<<"get">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"move">>,<<"key">>,<<"2">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"key">>]),
	erldis_client:sr_scall(Client,[<<"select">>,<<"2">>]),
	true = erldis_client:sr_scall(Client,[<<"exists">>,<<"key">>]),
	OldKey = erldis_client:sr_scall(Client,[<<"get">>,<<"key">>]).



pattern(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),

	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"foo">>,<<"bar">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"food">>,<<"bar">>]),
	Keys = erldis_client:scall(Client,[<<"keys">>,<<"*">>]),
	[] = (Keys -- [<<"foo">>, <<"food">>]).


randomkey(Config)-> 
	{client,Client} = lists:keyfind(client, 1, Config),

	ok = erldis_client:sr_scall(Client,[<<"flushdb">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"foo">>,<<"bar">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"food">>,<<"bar">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"fool">>,<<"bar">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"foot">>,<<"bar">>]),

	%% If 200 keys are chosen randomly, there is a high probability that every key has been chosen atleast once
	%% Will prove that every key in the db can be chosen by the randomkey function
	%% Will prove that the randomkey function does not always choose one specific key
	%% Will NOT prove that the randomkey function follows some other regular expression of a non-random nature

	Randoms = generate_random(200,Client),

	true = lists:member(<<"foo">>, Randoms),
	true = lists:member(<<"food">>, Randoms),
	true = lists:member(<<"fool">>, Randoms),
	true = lists:member(<<"foot">>, Randoms).