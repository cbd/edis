-module(strings_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
	[set_and_get,append,decr,incr,decrby,incrby,
	 setbit,getbit,setrange,getrange].
%%   getset,mget
%% 	 mset,msetnx,,setex,
%% 	 setnx,,strlen].

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

set_and_get(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	ok = erldis_client:sr_scall(Client, [<<"set">>,<<"name">>,<<"wrong name">>]),
	ok = erldis_client:sr_scall(Client, [<<"set">>,<<"name">>,<<"cool name">>]),
	<<"cool name">> = erldis_client:sr_scall(Client, [<<"get">>,<<"name">>]),
	nil = erldis_client:sr_scall(Client, [<<"get">>,<<"lastname">>]),
	
	LongString = list_to_binary(string:copies("abcd",100000)),
	ok = erldis_client:sr_scall(Client, [<<"set">>,<<"new_string">>,LongString]),
	LongString = erldis_client:sr_scall(Client, [<<"get">>,<<"new_string">>]),
	
	{error,<<"ERR wrong number of arguments for 'SET' command">>}  = erldis_client:sr_scall(Client, [<<"set">>,<<"name">>,<<"my">>,<<"name">>]),
	{error,<<"ERR wrong number of arguments for 'SET' command">>}  = erldis_client:sr_scall(Client, [<<"set">>,<<"name">>]),
	{error,<<"ERR wrong number of arguments for 'SET' command">>}  = erldis_client:sr_scall(Client, [<<"set">>]),
	    
	{error,<<"ERR wrong number of arguments for 'GET' command">>} = erldis_client:sr_scall(Client, [<<"get">>]),
	{error,<<"ERR wrong number of arguments for 'GET' command">>} = erldis_client:sr_scall(Client, [<<"get">>,<<"name">>,<<"andLastName">>]).
    %% TODO
	%% try to get a set, a hash, a sorted set and a list

append(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	10 = erldis_client:sr_scall(Client,[<<"append">>,<<"string">>,<<"my name is">>]),
	14 = erldis_client:sr_scall(Client,[<<"append">>,<<"string">>,<<" Tom">>]),
	
	{error,<<"ERR wrong number of arguments for 'APPEND' command">>} = erldis_client:sr_scall(Client,[<<"append">>,<<"string">>,<<"Tom">>,<<"Williams">>]),
	{error,<<"ERR wrong number of arguments for 'APPEND' command">>} = erldis_client:sr_scall(Client,[<<"append">>,<<"string">>]),
	
	<<"my name is Tom">> = erldis_client:sr_scall(Client, [<<"get">>,<<"string">>]).
	%% TODO
	%% try with a set, a hash, a sorted set and a list
	
decr(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"number">>,<<"4">>]),
	3 = erldis_client:sr_scall(Client,[<<"decr">>,<<"number">>]),
	<<"3">> = erldis_client:sr_scall(Client,[<<"get">>,<<"number">>]),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"number">>,<<"-20">>]),
	-21 = erldis_client:sr_scall(Client,[<<"decr">>,<<"number">>]),
	<<"-21">> = erldis_client:sr_scall(Client,[<<"get">>,<<"number">>]),
	
	-1 = erldis_client:sr_scall(Client,[<<"decr">>,<<"new_integer">>]),
	<<"-1">> = erldis_client:sr_scall(Client,[<<"get">>,<<"new_integer">>]),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"string">>,<<"it's not a integer">>]),
	{error,<<"ERR value is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"decr">>,<<"string">>]),
	
	{error,<<"ERR wrong number of arguments for 'DECR' command">>} = erldis_client:sr_scall(Client,[<<"decr">>,<<"number">>,<<"3">>]),
	{error,<<"ERR wrong number of arguments for 'DECR' command">>} = erldis_client:sr_scall(Client,[<<"decr">>]).

incr(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"number">>,<<"4">>]),
	5 = erldis_client:sr_scall(Client,[<<"incr">>,<<"number">>]),
	<<"5">> = erldis_client:sr_scall(Client,[<<"get">>,<<"number">>]),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"number">>,<<"-20">>]),
	-19 = erldis_client:sr_scall(Client,[<<"incr">>,<<"number">>]),
	<<"-19">> = erldis_client:sr_scall(Client,[<<"get">>,<<"number">>]),
	
	true = erldis_client:sr_scall(Client,[<<"incr">>,<<"new_integer">>]),
	<<"1">> = erldis_client:sr_scall(Client,[<<"get">>,<<"new_integer">>]),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"string">>,<<"it's not a integer">>]),
	{error,<<"ERR value is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"incr">>,<<"string">>]),
	
	{error,<<"ERR wrong number of arguments for 'INCR' command">>} = erldis_client:sr_scall(Client,[<<"incr">>,<<"number">>,<<"3">>]),
	{error,<<"ERR wrong number of arguments for 'INCR' command">>} = erldis_client:sr_scall(Client,[<<"incr">>]).

decrby(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"number">>,<<"40">>]),
	39 = erldis_client:sr_scall(Client,[<<"decrby">>,<<"number">>,<<"1">>]),
	24 = erldis_client:sr_scall(Client,[<<"decrby">>,<<"number">>,<<"15">>]),
	-6 = erldis_client:sr_scall(Client,[<<"decrby">>,<<"number">>,<<"30">>]),
	10 = erldis_client:sr_scall(Client,[<<"decrby">>,<<"number">>,<<"-16">>]),
	<<"10">> = erldis_client:sr_scall(Client,[<<"get">>,<<"number">>]),
	
	-80 = erldis_client:sr_scall(Client,[<<"decrby">>,<<"new_integer">>,<<"80">>]),
	<<"-80">> = erldis_client:sr_scall(Client,[<<"get">>,<<"new_integer">>]),
	120 = erldis_client:sr_scall(Client,[<<"decrby">>,<<"other_new_integer">>,<<"-120">>]),
	<<"120">> = erldis_client:sr_scall(Client,[<<"get">>,<<"other_new_integer">>]),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"string">>,<<"it's not a integer">>]),
	{error,<<"ERR value is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"decrby">>,<<"string">>,<<"20">>]),
	
	{error,<<"ERR wrong number of arguments for 'DECRBY' command">>} = erldis_client:sr_scall(Client,[<<"decrby">>,<<"number">>,<<"3">>,<<"4">>]),
	{error,<<"ERR wrong number of arguments for 'DECRBY' command">>} = erldis_client:sr_scall(Client,[<<"decrby">>,<<"number">>]).

incrby(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"number">>,<<"40">>]),
	41 = erldis_client:sr_scall(Client,[<<"incrby">>,<<"number">>,<<"1">>]),
	56 = erldis_client:sr_scall(Client,[<<"incrby">>,<<"number">>,<<"15">>]),
	-30 = erldis_client:sr_scall(Client,[<<"incrby">>,<<"number">>,<<"-86">>]),
	<<"-30">> = erldis_client:sr_scall(Client,[<<"get">>,<<"number">>]),
	
	80 = erldis_client:sr_scall(Client,[<<"incrby">>,<<"new_integer">>,<<"80">>]),
	<<"80">> = erldis_client:sr_scall(Client,[<<"get">>,<<"new_integer">>]),
	-120 = erldis_client:sr_scall(Client,[<<"incrby">>,<<"other_new_integer">>,<<"-120">>]),
	<<"-120">> = erldis_client:sr_scall(Client,[<<"get">>,<<"other_new_integer">>]),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"string">>,<<"it's not a integer">>]),
	{error,<<"ERR value is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"incrby">>,<<"string">>,<<"20">>]),
	
	{error,<<"ERR wrong number of arguments for 'INCRBY' command">>} = erldis_client:sr_scall(Client,[<<"incrby">>,<<"number">>,<<"3">>,<<"4">>]),
	{error,<<"ERR wrong number of arguments for 'INCRBY' command">>} = erldis_client:sr_scall(Client,[<<"incrby">>,<<"number">>]).

setbit(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	false = erldis_client:sr_scall(Client,[<<"setbit">>,<<"mykey">>,<<"1">>,<<"1">>]),
	<<2#01000000>> = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	false = erldis_client:sr_scall(Client,[<<"setbit">>,<<"mykey">>,<<"2">>,<<"1">>]),
	<<2#01100000>> = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	true = erldis_client:sr_scall(Client,[<<"setbit">>,<<"mykey">>,<<"1">>,<<"0">>]),
	<<2#00100000>> = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	
	%% Ascii "1" is integer 49 = 00 11 00 01
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"mykey">>,<<"1">>]),
	false = erldis_client:sr_scall(Client,[<<"setbit">>,<<"mykey">>,<<"6">>,<<"1">>]),
	<<2#00110011>> = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	true = erldis_client:sr_scall(Client,[<<"setbit">>,<<"mykey">>,<<"2">>,<<"0">>]),
	<<2#00010011>> = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	
	{error,<<"ERR bit offset is not an integer or out of range">>} = erldis_client:sr_scall(Client,[<<"setbit">>,<<"mykey">>,<<"-1">>,<<"0">>]),
	{error,<<"ERR wrong number of arguments for 'SETBIT' command">>} = erldis_client:sr_scall(Client,[<<"setbit">>,<<"mykey">>,<<"0">>]),
	{error,<<"ERR wrong number of arguments for 'SETBIT' command">>} = erldis_client:sr_scall(Client,[<<"setbit">>,<<"mykey">>,<<"0">>,<<"1">>,<<"1">>]).
	%% TODO
	%% set a list and try set one bit by setbit

getbit(Config) ->
    {client,Client} = lists:keyfind(client, 1, Config),
	
	false = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"0">>]),
	
	%% Single byte with 2nd and 3rd bit set
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"mykey">>,<<"`">>]),
	
	%% In-range
	false = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"0">>]),
	true = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"1">>]),
	true = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"2">>]),
	false = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"3">>]),
	
	%% Out-range 
	false = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"8">>]),
	false = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"800">>]),

	%% Ascii "1" is integer 49 = 00 11 00 01
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"mykey">>,<<"1">>]),
 	
	%% In-range
	false = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"0">>]),
	false = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"1">>]),
	true = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"2">>]),
	true = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"3">>]),
	
	%% Out-range 
	false = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"8">>]),
	false = erldis_client:sr_scall(Client,[<<"getbit">>,<<"mykey">>,<<"800">>]).

setrange(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	3 = erldis_client:sr_scall(Client,[<<"setrange">>,<<"mykey">>,<<"0">>,<<"foo">>]),
	<<"foo">> = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	
	4 = erldis_client:sr_scall(Client,[<<"setrange">>,<<"newkey">>,<<"1">>,<<"foo">>]),
	<<"\000foo">> = erldis_client:sr_scall(Client,[<<"get">>,<<"newkey">>]),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"mykey">>,<<"foo">>]),
	3 = erldis_client:sr_scall(Client,[<<"setrange">>,<<"mykey">>,<<"0">>,<<"b">>]),
	<<"boo">> = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	
	3 = erldis_client:sr_scall(Client,[<<"setrange">>,<<"mykey">>,<<"0">>,<<"">>]),
	<<"boo">> = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	
	3 = erldis_client:sr_scall(Client,[<<"setrange">>,<<"mykey">>,<<"1">>,<<"a">>]),
	<<"bao">> = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	
	7 = erldis_client:sr_scall(Client,[<<"setrange">>,<<"mykey">>,<<"4">>,<<"bar">>]),
	<<"bao\000bar">> = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	
	{error,<<"ERR offset is out of range">>} = erldis_client:sr_scall(Client,[<<"setrange">>,<<"mykey">>,<<"-1">>,<<"bar">>]),
	{error,<<"ERR wrong number of arguments for 'SETRANGE' command">>} = erldis_client:sr_scall(Client,[<<"setrange">>,<<"mykey">>,<<"4">>]),
	{error,<<"ERR wrong number of arguments for 'SETRANGE' command">>} = erldis_client:sr_scall(Client,[<<"setrange">>,<<"mykey">>,<<"4">>,<<"foo">>,<<"bar">>]).
	
getrange(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	nil = erldis_client:sr_scall(Client,[<<"getrange">>,<<"mykey">>,<<"0">>,<<"-1">>]),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"mykey">>,<<"Hello World">>]),
	<<"Hell">> = erldis_client:sr_scall(Client,[<<"getrange">>,<<"mykey">>,<<"0">>,<<"3">>]),
	<<"Hello World">> = erldis_client:sr_scall(Client,[<<"getrange">>,<<"mykey">>,<<"0">>,<<"-1">>]),
	nil = erldis_client:sr_scall(Client,[<<"getrange">>,<<"mykey">>,<<"5">>,<<"3">>]),
	<<"orld">> = erldis_client:sr_scall(Client,[<<"getrange">>,<<"mykey">>,<<"-4">>,<<"-1">>]),
	<<" World">> = erldis_client:sr_scall(Client,[<<"getrange">>,<<"mykey">>,<<"5">>,<<"100">>]),
	<<"Hello World">> = erldis_client:sr_scall(Client,[<<"getrange">>,<<"mykey">>,<<"-500">>,<<"100">>]).
