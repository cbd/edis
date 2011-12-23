%% @hidden
-module(strings_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [set_and_get,append,decr,incr,decrby,incrby,
	 setbit,getbit,setrange,getrange,mset,mget,
	 setnx,msetnx,setex,getset,strlen].

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

set_and_get(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	ok = erldis_client:sr_scall(Client, [<<"set">>,<<"string">>,<<"foo">>]),
	ok = erldis_client:sr_scall(Client, [<<"set">>,<<"string">>,<<"bar">>]),
	<<"bar">> = erldis_client:sr_scall(Client, [<<"get">>,<<"string">>]),
	nil = erldis_client:sr_scall(Client, [<<"get">>,<<"string2">>]),
	
	LongString = list_to_binary(string:copies("abcd",100000)),
	ok = erldis_client:sr_scall(Client, [<<"set">>,<<"new_string">>,LongString]),
	LongString = erldis_client:sr_scall(Client, [<<"get">>,<<"new_string">>]),

	%% Try to get a list
	3  = erldis_client:sr_scall(Client,[<<"rpush">>,<<"list">>,<<"a">>,<<"b">>,<<"c">>]),
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client, [<<"get">>,<<"list">>]),
	
	{error,<<"ERR wrong number of arguments for 'SET' command">>}  = erldis_client:sr_scall(Client, [<<"set">>,<<"string">>,<<"foo">>,<<"bar">>]),
	{error,<<"ERR wrong number of arguments for 'SET' command">>}  = erldis_client:sr_scall(Client, [<<"set">>,<<"string">>]),
	{error,<<"ERR wrong number of arguments for 'SET' command">>}  = erldis_client:sr_scall(Client, [<<"set">>]),
	    
	{error,<<"ERR wrong number of arguments for 'GET' command">>} = erldis_client:sr_scall(Client, [<<"get">>]),
	{error,<<"ERR wrong number of arguments for 'GET' command">>} = erldis_client:sr_scall(Client, [<<"get">>,<<"string">>,<<"string2">>]).

append(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	3 = erldis_client:sr_scall(Client,[<<"append">>,<<"string">>,<<"foo">>]),
	7 = erldis_client:sr_scall(Client,[<<"append">>,<<"string">>,<<" bar">>]),
	
	<<"foo bar">> = erldis_client:sr_scall(Client, [<<"get">>,<<"string">>]),

	%% Try with a list
	3  = erldis_client:sr_scall(Client,[<<"rpush">>,<<"list">>,<<"a">>,<<"b">>,<<"c">>]),
	{error,<<"ERR Operation against a key holding the wrong kind of value">>} = erldis_client:sr_scall(Client,[<<"append">>,<<"list">>,<<"foo">>]),

	{error,<<"ERR wrong number of arguments for 'APPEND' command">>} = erldis_client:sr_scall(Client,[<<"append">>,<<"string">>,<<"foo">>,<<"bar">>]),
	{error,<<"ERR wrong number of arguments for 'APPEND' command">>} = erldis_client:sr_scall(Client,[<<"append">>,<<"string">>]).
	
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

mset(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	ok = erldis_client:sr_scall(Client,[<<"mset">>,<<"x">>,<<"10">>,<<"y">>,<<"foo bar">>,<<"z">>,<<"x x x x x x x\n\n\r\n">>]),
	<<"10">> = erldis_client:sr_scall(Client,[<<"get">>,<<"x">>]),
	<<"foo bar">> = erldis_client:sr_scall(Client,[<<"get">>,<<"y">>]),
	<<"x x x x x x x\n\n\r\n">> = erldis_client:sr_scall(Client,[<<"get">>,<<"z">>]),
	
	{error,<<"ERR wrong number of arguments for 'MSET' command">>} = erldis_client:sr_scall(Client,[<<"mset">>,<<"x">>,<<"10">>,<<"y">>,<<"foo bar">>,<<"z">>]).
	
mget(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"foo">>,<<"FOO">>]),
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"bar">>,<<"BAR">>]),
	[<<"BAR">>,<<"FOO">>] = erldis_client:scall(Client,[<<"mget">>,<<"bar">>,<<"foo">>]),
	[<<"BAR">>,nil,<<"FOO">>] = erldis_client:scall(Client,[<<"mget">>,<<"bar">>,<<"bazz">>,<<"foo">>]),
	[<<"FOO">>,nil,<<"BAR">>,nil] = erldis_client:scall(Client,[<<"mget">>,<<"foo">>,<<"bazz">>,<<"bar">>,<<"myset">>]),
	
	{error,<<"ERR wrong number of arguments for 'MGET' command">>} = erldis_client:sr_scall(Client,[<<"mget">>]).

setnx(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	%% with not existing key
	true = erldis_client:sr_scall(Client,[<<"setnx">>,<<"x">>,<<"10">>]),
	<<"10">> = erldis_client:sr_scall(Client,[<<"get">>,<<"x">>]),
	
	%% with already existent key
	false = erldis_client:sr_scall(Client,[<<"setnx">>,<<"x">>,<<"35">>]),
	<<"10">> = erldis_client:sr_scall(Client,[<<"get">>,<<"x">>]),
	
	{error,<<"ERR wrong number of arguments for 'SETNX' command">>} = erldis_client:sr_scall(Client,[<<"setnx">>,<<"x">>]).
	
msetnx(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),    
	
	%% with not existing keys
	true = erldis_client:sr_scall(Client,[<<"msetnx">>,<<"x">>,<<"10">>,<<"y">>,<<"foo bar">>]),
	<<"10">> = erldis_client:sr_scall(Client,[<<"get">>,<<"x">>]),
	<<"foo bar">> = erldis_client:sr_scall(Client,[<<"get">>,<<"y">>]),
	
	%% with already existent key
	false = erldis_client:sr_scall(Client,[<<"msetnx">>,<<"x">>,<<"22">>,<<"w">>,<<"foo">>,<<"z">>,<<"bar">>]),
	<<"10">> = erldis_client:sr_scall(Client,[<<"get">>,<<"x">>]),
	nil = erldis_client:sr_scall(Client,[<<"get">>,<<"w">>]),
	nil = erldis_client:sr_scall(Client,[<<"get">>,<<"z">>]),
	
	{error,<<"ERR wrong number of arguments for 'MSETNX' command">>} = erldis_client:sr_scall(Client,[<<"msetnx">>,<<"x">>,<<"22">>,<<"w">>]),
	{error,<<"ERR wrong number of arguments for 'MSETNX' command">>} = erldis_client:sr_scall(Client,[<<"msetnx">>]).
	
setex(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	ok = erldis_client:sr_scall(Client,[<<"setex">>,<<"mykey">>,<<"6">>,<<"FOO">>]),
	<<"FOO">> = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	timer:sleep(3000),
	<<"FOO">> = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	timer:sleep(4500),
	nil = erldis_client:sr_scall(Client,[<<"get">>,<<"mykey">>]),
	
	{error,<<"ERR invalid expire time in SETEX">>} = erldis_client:sr_scall(Client,[<<"setex">>,<<"z">>,<<"-30">>,<<"bar">>]),
	{error,<<"ERR wrong number of arguments for 'SETEX' command">>} = erldis_client:sr_scall(Client,[<<"setex">>,<<"z">>,<<"5">>]).

getset(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	%% Set new value
    nil = erldis_client:sr_scall(Client,[<<"getset">>,<<"foo">>,<<"xyz">>]),
	<<"xyz">> = erldis_client:sr_scall(Client,[<<"get">>,<<"foo">>]),

	%% Replace old value
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"foo">>,<<"bar">>]),
    <<"bar">> = erldis_client:sr_scall(Client,[<<"getset">>,<<"foo">>,<<"buzz">>]),    
	<<"buzz">> = erldis_client:sr_scall(Client,[<<"get">>,<<"foo">>]),
    
	{error,<<"ERR wrong number of arguments for 'GETSET' command">>} = erldis_client:sr_scall(Client,[<<"getset">>,<<"foo">>]),
	{error,<<"ERR wrong number of arguments for 'GETSET' command">>} = erldis_client:sr_scall(Client,[<<"getset">>,<<"foo">>,<<"bar">>,<<"buzz">>]).

strlen(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	
	%% With already existent key
	ok = erldis_client:sr_scall(Client,[<<"set">>,<<"mykey">>,<<"Hello World">>]),
	11 = erldis_client:sr_scall(Client,[<<"strlen">>,<<"mykey">>]),
	
	%% With not existing key
	false = erldis_client:sr_scall(Client,[<<"strlen">>,<<"nonexisting">>]),
	
	{error,<<"ERR wrong number of arguments for 'STRLEN' command">>} = erldis_client:sr_scall(Client,[<<"strlen">>,<<"foo">>,<<"bar">>]),
	{error,<<"ERR wrong number of arguments for 'STRLEN' command">>} = erldis_client:sr_scall(Client,[<<"strlen">>]).