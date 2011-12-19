%% @hidden
-module(zsets_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-define(ERR_NUM_ARGS(Command), {error,<<"ERR wrong number of arguments for '",Command/binary,"' command">>}).
-define(ERR_NOTDOUBLE, {error,<<"ERR value is not a double">>}).
-define(ERR_MIN_MAX_NOTDOUBLE, {error,<<"ERR min or max is not a double">>}).
-define(ERR_SYNTAX, {error,<<"ERR syntax error">>}).
-define(ERR_BAD_KEY, {error,<<"ERR Operation against a key holding the wrong kind of value">>}).
-define(ERR_NOT_INTEGER, {error,<<"ERR value is not an integer or out of range">>}).
-define(ERR_KEY_NEEDED,{error, <<"ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE">>}).
-define(ERR_WEIGHT_VALUE,{error,<<"ERR weight value is not a double">>}).
-define(ERR_NEG_LENGTH,{error,<<"ERR negative length (-1)">>}).

all() -> [zadd,zincrby,zcard,zrem,zrange,
					zrevrange,zrank_zrevrank,zcount,
					zrangebyscore_zrevrangebyscore,
					zremrangebyscore,zremrangebyrank,
					zunionstore,zinterstore].

init_per_suite(Config) ->
	{ok,Client} = connect_erldis(10),
	NewConfig = lists:keystore(client,1,Config,{client,Client}),
	NewConfig.

init_per_testcase(_TestCase,Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	erldis_client:sr_scall(Client,[<<"flushdb">>]),
	erldis_client:sr_scall(Client,[<<"set">>,<<"string">>,<<"I am not a sorted set">>]),
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

zadd(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS = ?ERR_NUM_ARGS(<<"ZADD">>),
	%% basic ZADD and score update
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,10,<<"x">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,20,<<"y">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,30,<<"z">>]),
	[<<"x">>,<<"y">>,<<"z">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,0,-1]),
	false = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,1,<<"y">>]),
	[<<"y">>,<<"x">>,<<"z">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,0,-1]),
	%% element can't be set to NaN with ZADD
	?ERR_NOTDOUBLE = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,<<"nan">>,<<"x">>]),
	%% Variadic version base case
	3 = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset">>,10,<<"x">>,20,<<"y">>,30,<<"z">>]),
	[<<"x">>,<<"10">>,<<"y">>,<<"20">>,<<"z">>,<<"30">>] = erldis_client:scall(Client,[<<"zrange">>,<<"myset">>,0,-1,<<"withscores">>]),
	%% Return value is the number of actually added items
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset">>,5,<<"a">>,20,<<"y">>,30,<<"z">>]),
	[<<"a">>,<<"5">>,<<"x">>,<<"10">>,<<"y">>,<<"20">>,<<"z">>,<<"30">>] = erldis_client:scall(Client,[<<"zrange">>,<<"myset">>,0,-1,<<"withscores">>]),
	%% Variadic version does not add nothing on single parsing err
	true = erldis_client:sr_scall(Client,[<<"del">>,<<"myset">>]),
	?ERR_NOTDOUBLE = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset">>,10,<<"x">>,20,<<"y">>,<<"thirty">>,<<"z">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"myset">>]),
	%% Variadic version will raise error on missing arg
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset2">>,10,<<"x">>,20]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset2">>,10,<<"x">>,20,<<"y">>,30]),
	%% Against non zset
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zadd">>,<<"string">>,55,<<"xyz">>]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zadd">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset2">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset2">>,1]).

zincrby(Config) ->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS = ?ERR_NUM_ARGS(<<"ZINCRBY">>),
	%% Non existing zset
	<<"5">> = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"newzset">>,5,<<"xyz">>]),
	[<<"xyz">>] = erldis_client:scall(Client,[<<"zrange">>,<<"newzset">>,0,-1]),
	%% Increment and decrement
	<<"2">> = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"zset">>,2,<<"foo">>]),
	<<"1">> = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"zset">>,1,<<"bar">>]),
	[<<"bar">>,<<"foo">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
	<<"11">> = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"zset">>,10,<<"bar">>]),
	<<"-3">> = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"zset">>,-5,<<"foo">>]),
	<<"6">> = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"zset">>,-5,<<"bar">>]),
	[<<"foo">>,<<"bar">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
	<<"-3">> = erldis_client:sr_scall(Client,[<<"zscore">>,<<"zset">>,<<"foo">>]),
	<<"6">> = erldis_client:sr_scall(Client,[<<"zscore">>,<<"zset">>,<<"bar">>]),
	%% Element can't be set to NaN with ZINCRBY
	?ERR_NOTDOUBLE = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"ztmp">>,<<"nan">>,<<"x">>]),
	%% ZINCRBY does not work variadic 
  ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"myset">>,10,<<"x">>,20,<<"y">>,30,<<"z">>]),
	%% Against non zset
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"string">>,10,<<"x">>]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zincrby">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"myset">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"myset">>,10]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zincrby">>,<<"myset">>,10,<<"x">>,20]).

zcard(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS = ?ERR_NUM_ARGS(<<"ZCARD">>),
	3 = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset">>,10,<<"x">>,20,<<"y">>,30,<<"z">>]),
	3 = erldis_client:sr_scall(Client,[<<"zcard">>,<<"myset">>]),
  false = erldis_client:sr_scall(Client,[<<"zcard">>,<<"otherset">>]),
	%% Against non zset
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zcard">>,<<"string">>]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zcard">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zcard">>,<<"myset">>,<<"otherset">>]).

zrem(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS = ?ERR_NUM_ARGS(<<"ZREM">>),
	%%  Remove key after last element is removed
	2 = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset">>,10,<<"foo">>,20,<<"bar">>]),
	true = erldis_client:sr_scall(Client,[<<"exists">>,<<"myset">>]),
	false = erldis_client:sr_scall(Client,[<<"zrem">>,<<"myset">>,<<"buzz">>]),
	true = erldis_client:sr_scall(Client,[<<"zrem">>,<<"myset">>,<<"foo">>]),
	true = erldis_client:sr_scall(Client,[<<"zrem">>,<<"myset">>,<<"bar">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"myset">>]),
	%% Variadic version
	3 = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zset">>,10,<<"a">>,20,<<"b">>,3,<<"c">>]),
	2 = erldis_client:sr_scall(Client,[<<"zrem">>,<<"zset">>,<<"x">>,<<"a">>,<<"y">>,<<"b">>,<<"k">>]),
	false = erldis_client:sr_scall(Client,[<<"zrem">>,<<"zset">>,<<"foo">>,<<"bar">>]),
	true = erldis_client:sr_scall(Client,[<<"zrem">>,<<"zset">>,<<"c">>]),
	false = erldis_client:sr_scall(Client,[<<"exists">>,<<"zset">>]),
	%% Variadic version -- remove elements after key deletion
	erldis_client:sr_scall(Client,[<<"del">>,<<"zset">>]),
	3 = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zset">>,10,<<"a">>,20,<<"b">>,3,<<"c">>]),
	3 = erldis_client:sr_scall(Client,[<<"zrem">>,<<"zset">>,<<"x">>,<<"a">>,<<"c">>,<<"b">>,<<"k">>]),
	%% Non existing zset
	false = erldis_client:sr_scall(Client,[<<"zrem">>,<<"nonexist">>,<<"x">>]),
	%% Against non zset
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zrem">>,<<"string">>,<<"x">>]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zrem">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zrem">>,<<"zset">>]).
	
zrange(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS = ?ERR_NUM_ARGS(<<"ZRANGE">>),
	4 = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,1,<<"a">>,2,<<"b">>,3,<<"c">>,4,<<"d">>]),
	%% Basic
	[<<"a">>,<<"b">>,<<"c">>,<<"d">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,0,-1]),
	[<<"a">>,<<"b">>,<<"c">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,0,-2]),
	[<<"b">>,<<"c">>,<<"d">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,1,-1]),
	[<<"b">>,<<"c">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,1,-2]),
	[<<"c">>,<<"d">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,-2,-1]),
	[<<"c">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,-2,-2]),
	%% Out of range start index
	[<<"a">>,<<"b">>,<<"c">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,-5,2]),
	[<<"a">>,<<"b">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,-5,1]),
	[] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,5,-1]),
	[] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,5,-2]),
	%% Out of range end index
  [<<"a">>,<<"b">>,<<"c">>,<<"d">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,0,5]),
	[<<"b">>,<<"c">>,<<"d">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,1,5]),
	[] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,0,-5]),
	[] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,1,-5]),
	%% Withscores
	[<<"a">>,<<"1">>,<<"b">>,<<"2">>,<<"c">>,<<"3">>,<<"d">>,<<"4">>] = erldis_client:scall(Client,[<<"zrange">>,<<"ztmp">>,0,-1,<<"withscores">>]),
	%% Non existing zset
	[] = erldis_client:scall(Client,[<<"zrange">>,<<"nonexist">>,0,-1]),
	%% Against non zset
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zrange">>,<<"string">>,0,-1]),
	%% Syntax error
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zrange">>,<<"ztmp">>,0,-1,<<"withoutscores">>]),
	?ERR_NOT_INTEGER = erldis_client:sr_scall(Client,[<<"zrange">>,<<"ztmp">>,0,<<"two">>]),
	?ERR_NOT_INTEGER = erldis_client:sr_scall(Client,[<<"zrange">>,<<"ztmp">>,<<"one">>,4]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zrange">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zrange">>,<<"ztmp">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zrange">>,<<"ztmp">>,0]).

zrevrange(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS = ?ERR_NUM_ARGS(<<"ZREVRANGE">>),
	4 = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,1,<<"a">>,2,<<"b">>,3,<<"c">>,4,<<"d">>]),
	%% Basic
	[<<"d">>,<<"c">>,<<"b">>,<<"a">>] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,0,-1]),
	[<<"d">>,<<"c">>,<<"b">>] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,0,-2]),
	[<<"c">>,<<"b">>,<<"a">>] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,1,-1]),
	[<<"c">>,<<"b">>] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,1,-2]),
	[<<"b">>,<<"a">>] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,-2,-1]),
	[<<"b">>] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,-2,-2]),
	%% Out of range start index
	[<<"d">>,<<"c">>,<<"b">>] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,-5,2]),
	[<<"d">>,<<"c">>] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,-5,1]),
	[] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,5,-1]),
	[] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,5,-2]),
	%% Out of range end index
	[<<"d">>,<<"c">>,<<"b">>,<<"a">>] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,0,5]),
	[<<"c">>,<<"b">>,<<"a">>] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,1,5]),
	[] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,0,-5]),
	[] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,1,-5]),
	%% Withscores            
	[<<"d">>,<<"4">>,<<"c">>,<<"3">>,<<"b">>,<<"2">>,<<"a">>,<<"1">>] = erldis_client:scall(Client,[<<"zrevrange">>,<<"ztmp">>,0,-1,<<"withscores">>]),
	%% Non existing zset
	[] = erldis_client:scall(Client,[<<"zrevrange">>,<<"nonexist">>,0,-1]),
	%% Against non zset
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zrevrange">>,<<"string">>,0,-1]),
	%% Syntax error
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zrevrange">>,<<"ztmp">>,0,-1,<<"withoutscores">>]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zrevrange">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zrevrange">>,<<"ztmp">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zrevrange">>,<<"ztmp">>,0]).

zrank_zrevrank(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS_ZRANK = ?ERR_NUM_ARGS(<<"ZRANK">>),
	ERR_NUM_ARGS_ZREVRANK = ?ERR_NUM_ARGS(<<"ZREVRANK">>),
	%% Basic
	3 = erldis_client:sr_scall(Client,[<<"zadd">>,<<"ztmp">>,10,<<"x">>,20,<<"y">>,30,<<"z">>]),
	false = erldis_client:sr_scall(Client,[<<"zrank">>,<<"ztmp">>,<<"x">>]),
	true = erldis_client:sr_scall(Client,[<<"zrank">>,<<"ztmp">>,<<"y">>]),
	2 = erldis_client:sr_scall(Client,[<<"zrank">>,<<"ztmp">>,<<"z">>]),
	nil = erldis_client:sr_scall(Client,[<<"zrank">>,<<"ztmp">>,<<"foo">>]),
	2 = erldis_client:sr_scall(Client,[<<"zrevrank">>,<<"ztmp">>,<<"x">>]),
	true = erldis_client:sr_scall(Client,[<<"zrevrank">>,<<"ztmp">>,<<"y">>]),
	false = erldis_client:sr_scall(Client,[<<"zrevrank">>,<<"ztmp">>,<<"z">>]),
	nil = erldis_client:sr_scall(Client,[<<"zrevrank">>,<<"ztmp">>,<<"foo">>]),
	%% After deletion	
	true = erldis_client:sr_scall(Client,[<<"zrem">>,<<"ztmp">>,<<"y">>]),
	false = erldis_client:sr_scall(Client,[<<"zrank">>,<<"ztmp">>,<<"x">>]),
	true = erldis_client:sr_scall(Client,[<<"zrank">>,<<"ztmp">>,<<"z">>]),
	true = erldis_client:sr_scall(Client,[<<"zrevrank">>,<<"ztmp">>,<<"x">>]),
	false = erldis_client:sr_scall(Client,[<<"zrevrank">>,<<"ztmp">>,<<"z">>]),
	%% Non existing zset
	nil = erldis_client:sr_scall(Client,[<<"zrank">>,<<"nonexist">>,<<"x">>]),
	nil = erldis_client:sr_scall(Client,[<<"zrevrank">>,<<"nonexist">>,<<"x">>]),
	%% Against non zset
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zrank">>,<<"string">>,<<"x">>]),
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zrevrank">>,<<"string">>,<<"x">>]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS_ZRANK = erldis_client:sr_scall(Client,[<<"zrank">>]),
	ERR_NUM_ARGS_ZRANK = erldis_client:sr_scall(Client,[<<"zrank">>,<<"ztmp">>]),
	ERR_NUM_ARGS_ZRANK = erldis_client:sr_scall(Client,[<<"zrank">>,<<"ztmp">>,<<"x">>,<<"z">>]),
	ERR_NUM_ARGS_ZREVRANK = erldis_client:sr_scall(Client,[<<"zrevrank">>]),
	ERR_NUM_ARGS_ZREVRANK = erldis_client:sr_scall(Client,[<<"zrevrank">>,<<"ztmp">>]),
	ERR_NUM_ARGS_ZREVRANK = erldis_client:sr_scall(Client,[<<"zrevrank">>,<<"ztmp">>,<<"x">>,<<"z">>]).

zcount(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS = ?ERR_NUM_ARGS(<<"ZCOUNT">>),
	7 = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset">>,-50,<<"a">>,1,<<"b">>,
																		 2,<<"c">>,3,<<"d">>,4,<<"e">>,5,<<"f">>,50,<<"g">>]),
	%% Inclusive range
	3 = erldis_client:sr_scall(Client,[<<"zcount">>,<<"myset">>,0,3]),
	%% Exclusive range
	2 = erldis_client:sr_scall(Client,[<<"zcount">>,<<"myset">>,<<"(0">>,<<"(3">>]),
	%% Non existing zset
	false = erldis_client:sr_scall(Client,[<<"zcount">>,<<"nonexist">>,0,3]),
	%% Against non zset
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zcount">>,<<"string">>,0,3]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zcount">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zcount">>,<<"myset">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zcount">>,<<"myset">>,0]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zcount">>,<<"myset">>,0,1,2]).
	
zrangebyscore_zrevrangebyscore(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS_ZRANGEBYSCORE = ?ERR_NUM_ARGS(<<"ZRANGEBYSCORE">>),
	ERR_NUM_ARGS_ZREVRANGEBYSCORE = ?ERR_NUM_ARGS(<<"ZREVRANGEBYSCORE">>),
	7 = erldis_client:sr_scall(Client,[<<"zadd">>,<<"myset">>,-50,<<"a">>,1,<<"b">>,
																		 2,<<"c">>,3,<<"d">>,4,<<"e">>,5,<<"f">>,50,<<"g">>]),
	%% Inclusive range
 	[<<"a">>,<<"b">>,<<"c">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"-inf">>,2]),
	[<<"b">>,<<"c">>,<<"d">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,0,3]),
	[<<"d">>,<<"e">>,<<"f">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,3,6]),
	[<<"e">>,<<"f">>,<<"g">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,4,<<"+inf">>]),
	[<<"c">>,<<"b">>,<<"a">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,2,<<"-inf">>]),
	[<<"d">>,<<"c">>,<<"b">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,3,0]),
	[<<"f">>,<<"e">>,<<"d">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,6,3]),
	[<<"g">>,<<"f">>,<<"e">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,<<"+inf">>,4]),
	%% Exclusive range
	[<<"a">>,<<"b">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"(-inf">>,<<"(2">>]),
	[<<"b">>,<<"c">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"(0">>,<<"(3">>]),
	[<<"e">>,<<"f">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"(3">>,<<"(6">>]),
	[<<"f">>,<<"g">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"(4">>,<<"(inf">>]),
	[<<"b">>,<<"a">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,<<"(2">>,<<"(-inf">>]),
	[<<"c">>,<<"b">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,<<"(3">>,<<"(0">>]),
	[<<"f">>,<<"e">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,<<"(6">>,<<"(3">>]),
	[<<"g">>,<<"f">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,<<"(inf">>,<<"(4">>]),
	%% Empty ranges - Inclusive
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,4,2]),
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,51,<<"inf">>]),
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"-inf">>,-51]),
	[] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,<<"+inf">>,51]),
	[] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,-51,<<"-inf">>]),
	%% Empty ranges - Exclusive
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"(4">>,<<"(2">>]),
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"2">>,<<"(2">>]),
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"(2">>,<<"2">>]),
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"(50">>,<<"(inf">>]),
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"(-inf">>,<<"(-51">>]),
	[] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,<<"(+inf">>,<<"(51">>]),
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"(-51">>,<<"(-inf">>]),
	%% Empty inner range
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,2.4,2.6]),
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"(2.4">>,2.6]),
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,2.4,<<"(2.6">>]),
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"(2.4">>,<<"(2.6">>]),
	%% With WITHSCORES
	[<<"b">>,<<"1">>,<<"c">>,<<"2">>,<<"d">>,<<"3">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,0,3,<<"withscores">>]),
	[<<"d">>,<<"3">>,<<"c">>,<<"2">>,<<"b">>,<<"1">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,3,0,<<"withscores">>]),
	%% With LIMIT
	[<<"b">>,<<"c">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,0,10,<<"LIMIT">>,0,2]),
	[<<"d">>,<<"e">>,<<"f">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,0,10,<<"LIMIT">>,2,3]),
	[<<"d">>,<<"e">>,<<"f">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,0,10,<<"LIMIT">>,2,10]),
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,0,10,<<"LIMIT">>,20,10]),
	[<<"f">>,<<"e">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,10,0,<<"LIMIT">>,0,2]),
	[<<"d">>,<<"c">>,<<"b">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,10,0,<<"LIMIT">>,2,3]),
	[<<"d">>,<<"c">>,<<"b">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,10,0,<<"LIMIT">>,2,10]),
	[] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,10,0,<<"LIMIT">>,20,10]),
	%% With LIMIT and WITHSCORES
	[<<"e">>,<<"4">>,<<"f">>,<<"5">>] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"myset">>,2,5,<<"LIMIT">>,2,3,<<"withscores">>]),
	[<<"d">>,<<"3">>,<<"c">>,<<"2">>] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,5,2,<<"LIMIT">>,2,3,<<"withscores">>]),
	%% Non existing zset
	[] = erldis_client:scall(Client,[<<"zrangebyscore">>,<<"nonexist">>,2,5]),
	[] = erldis_client:scall(Client,[<<"zrevrangebyscore">>,<<"nonexist">>,5,2]),
	%% Against non zset
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zrangebyscore">>,<<"string">>,0,3]),
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zrevrangebyscore">>,<<"string">>,0,3]),
	%% Syntax error
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zrangebyscore">>,<<"myset">>,2,4,8]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,2,4,8]),
	?ERR_NOTDOUBLE = erldis_client:sr_scall(Client,[<<"zrangebyscore">>,<<"myset">>,<<"two">>,4]),
	?ERR_NOTDOUBLE = erldis_client:sr_scall(Client,[<<"zrangebyscore">>,<<"myset">>,2,<<"four">>]),
	?ERR_NOTDOUBLE = erldis_client:sr_scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,<<"two">>,4]),
	?ERR_NOTDOUBLE = erldis_client:sr_scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,2,<<"four">>]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS_ZRANGEBYSCORE = erldis_client:sr_scall(Client,[<<"zrangebyscore">>]),
	ERR_NUM_ARGS_ZRANGEBYSCORE = erldis_client:sr_scall(Client,[<<"zrangebyscore">>,<<"myset">>]),
	ERR_NUM_ARGS_ZRANGEBYSCORE = erldis_client:sr_scall(Client,[<<"zrangebyscore">>,<<"myset">>,2]),
	ERR_NUM_ARGS_ZREVRANGEBYSCORE = erldis_client:sr_scall(Client,[<<"zrevrangebyscore">>]),
	ERR_NUM_ARGS_ZREVRANGEBYSCORE = erldis_client:sr_scall(Client,[<<"zrevrangebyscore">>,<<"myset">>]),
	ERR_NUM_ARGS_ZREVRANGEBYSCORE = erldis_client:sr_scall(Client,[<<"zrevrangebyscore">>,<<"myset">>,2]).

%% Creates a default zset and runs the command zremrangebyscore on it.
-spec remrangebyscore(pid(),integer()|binary(),integer()|binary()) -> integer()|boolean().
remrangebyscore(Client,Min,Max) ->	
	erldis_client:sr_scall(Client,[<<"del">>,<<"zset">>]),
	erldis_client:sr_scall(Client,[<<"zadd">>,<<"zset">>,1,<<"a">>,2,<<"b">>,
																 3,<<"c">>,4,<<"d">>,5,<<"e">>]),
	true = erldis_client:sr_scall(Client,[<<"exists">>,<<"zset">>]),
	erldis_client:sr_scall(Client,[<<"zremrangebyscore">>,<<"zset">>,Min,Max]).

zremrangebyscore(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS = ?ERR_NUM_ARGS(<<"ZREMRANGEBYSCORE">>),
	%% Inner range
  3 = remrangebyscore(Client,2,4),
  [<<"a">>,<<"e">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
	%% Start underflow
	true = remrangebyscore(Client,-10,1),
  [<<"b">>,<<"c">>,<<"d">>,<<"e">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
  %% End overflow
  true = remrangebyscore(Client,5,10),
  [<<"a">>,<<"b">>,<<"c">>,<<"d">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
  %% Switch min and max
  false = remrangebyscore(Client,4,2),
  [<<"a">>,<<"b">>,<<"c">>,<<"d">>,<<"e">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
	%% -inf to mid
	3 = remrangebyscore(Client,<<"-inf">>,3),
  [<<"d">>,<<"e">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
  %% Mid to +inf
  3 = remrangebyscore(Client,3,<<"+inf">>),
  [<<"a">>,<<"b">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
  %% -inf to +inf
  5 = remrangebyscore(Client,<<"-inf">>,<<"+inf">>),
  [] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),  
  %% Exclusive min
  4 = remrangebyscore(Client,<<"(1">>,5),
  [<<"a">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
	3 = remrangebyscore(Client,<<"(2">>,5),
  [<<"a">>,<<"b">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
  %% Exclusive max
	4 = remrangebyscore(Client,1,<<"(5">>),
  [<<"e">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
  3 = remrangebyscore(Client,1,<<"(4">>),
  [<<"d">>,<<"e">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
  %% Exclusive min and max
  3 = remrangebyscore(Client,<<"(1">>,<<"(5">>),
  [<<"a">>,<<"e">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),    
  %% Destroy when empty
	5 = remrangebyscore(Client,1,5),
  false = erldis_client:sr_scall(Client,[<<"exists">>,<<"zset">>]),
	%% With non-value min or max
	?ERR_MIN_MAX_NOTDOUBLE = erldis_client:sr_scall(Client,[<<"zremrangebyscore">>,<<"zset">>,<<"one">>,4]),
	?ERR_MIN_MAX_NOTDOUBLE = erldis_client:sr_scall(Client,[<<"zremrangebyscore">>,<<"zset">>,1,<<"four">>]),
	%% Non existing zset
	false = erldis_client:sr_scall(Client,[<<"zremrangebyscore">>,<<"nonexist">>,2,5]),
	%% Against non zset
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zremrangebyscore">>,<<"string">>,0,3]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zremrangebyscore">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zremrangebyscore">>,<<"zset">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zremrangebyscore">>,<<"zset">>,2]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zremrangebyscore">>,<<"zset">>,3,5,8]).

%% Creates a default zset and runs the command zremrangebyrank on it.
-spec remrangebyrank(pid(),integer()|binary(),integer()|binary()) -> integer()|boolean().
remrangebyrank(Client,Min,Max) ->	
	erldis_client:sr_scall(Client,[<<"del">>,<<"zset">>]),
	erldis_client:sr_scall(Client,[<<"zadd">>,<<"zset">>,1,<<"a">>,2,<<"b">>,
																 3,<<"c">>,4,<<"d">>,5,<<"e">>]),
	true = erldis_client:sr_scall(Client,[<<"exists">>,<<"zset">>]),
	erldis_client:sr_scall(Client,[<<"zremrangebyrank">>,<<"zset">>,Min,Max]).

zremrangebyrank(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS = ?ERR_NUM_ARGS(<<"ZREMRANGEBYRANK">>),
	%% Inner range
  3 = remrangebyrank(Client,1,3),
  [<<"a">>,<<"e">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
  %% Start underflow
	true = remrangebyrank(Client,-10,0),
  [<<"b">>,<<"c">>,<<"d">>,<<"e">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
  %% Start overflow
  false = remrangebyrank(Client,10,-1),
  [<<"a">>,<<"b">>,<<"c">>,<<"d">>,<<"e">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
	%% End underflow
	false = remrangebyrank(Client,0,-10),
  [<<"a">>,<<"b">>,<<"c">>,<<"d">>,<<"e">>] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),
  %% End overflow
  5 = remrangebyrank(Client,0,10),
  [] = erldis_client:scall(Client,[<<"zrange">>,<<"zset">>,0,-1]),    
  %% Destroy when empty
  5 = remrangebyrank(Client,0,4),
  false = erldis_client:sr_scall(Client,[<<"exists">>,<<"zset">>]),    
	%% With non-value min or max
	?ERR_NOT_INTEGER = erldis_client:sr_scall(Client,[<<"zremrangebyrank">>,<<"zset">>,<<"one">>,4]),
	?ERR_NOT_INTEGER = erldis_client:sr_scall(Client,[<<"zremrangebyrank">>,<<"zset">>,1,<<"four">>]),
	%% Non existing zset
	false = erldis_client:sr_scall(Client,[<<"zremrangebyrank">>,<<"nonexist">>,2,5]),
	%% Against non zset
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zremrangebyrank">>,<<"string">>,0,3]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zremrangebyrank">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zremrangebyrank">>,<<"zset">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zremrangebyrank">>,<<"zset">>,2]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zremrangebyrank">>,<<"zset">>,3,5,8]).

zunionstore(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS = ?ERR_NUM_ARGS(<<"ZUNIONSTORE">>),
	%% Against non-existing key doesn't set destination 
  false = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"dst_key">>,1,<<"zseta">>]),
  false = erldis_client:sr_scall(Client,[<<"exists">>,<<"dst_key">>]),
	%% With empty set
	2 =	erldis_client:sr_scall(Client,[<<"zadd">>,<<"zseta">>,1,<<"a">>,2,<<"b">>]),
  2 = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>]),
	[<<"a">>,<<"1">>,<<"b">>,<<"2">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% Basics	
	2 = erldis_client:sr_scall(Client,[<<"del">>,<<"zsetc">>,<<"zseta">>,<<"zsetb">>]),
	3 =	erldis_client:sr_scall(Client,[<<"zadd">>,<<"zseta">>,1,<<"a">>,2,<<"b">>,3,<<"c">>]),
	3 =	erldis_client:sr_scall(Client,[<<"zadd">>,<<"zsetb">>,1,<<"b">>,2,<<"c">>,3,<<"d">>]),
	4 = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>]),
	[<<"a">>,<<"1">>,<<"b">>,<<"3">>,<<"d">>,<<"3">>,<<"c">>,<<"5">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With weights
	4 = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,2,3]),
 [<<"a">>,<<"2">>,<<"b">>,<<"7">>,<<"d">>,<<"9">>,<<"c">>,<<"12">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With a regular set and weights
	3 =	erldis_client:sr_scall(Client,[<<"sadd">>,<<"seta">>,<<"a">>,<<"b">>,<<"c">>]),
	4 = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"seta">>,<<"zsetb">>,<<"weights">>,2,3]),
	[<<"a">>,<<"2">>,<<"b">>,<<"5">>,<<"c">>,<<"8">>,<<"d">>,<<"9">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With AGGREGATE MIN
	4 = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"aggregate">>,<<"min">>]),
	[<<"a">>,<<"1">>,<<"b">>,<<"1">>,<<"c">>,<<"2">>,<<"d">>,<<"3">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With AGGREGATE MAX
	4 = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"aggregate">>,<<"max">>]),
  [<<"a">>,<<"1">>,<<"b">>,<<"2">>,<<"c">>,<<"3">>,<<"d">>,<<"3">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With WEIGHTS and AGGREGATE MIN
	4 = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,2,3,<<"aggregate">>,<<"min">>]),
  [<<"a">>,<<"2">>,<<"b">>,<<"3">>,<<"c">>,<<"6">>,<<"d">>,<<"9">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	4 = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,4,3,<<"aggregate">>,<<"min">>]),
  [<<"b">>,<<"3">>,<<"a">>,<<"4">>,<<"c">>,<<"6">>,<<"d">>,<<"9">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With WEIGHTS and AGGREGATE MAX
	4 = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,2,3,<<"aggregate">>,<<"max">>]),
  [<<"a">>,<<"2">>,<<"b">>,<<"4">>,<<"c">>,<<"6">>,<<"d">>,<<"9">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	4 = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,4,3,<<"aggregate">>,<<"max">>]),
  [<<"a">>,<<"4">>,<<"b">>,<<"8">>,<<"d">>,<<"9">>,<<"c">>,<<"12">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With +inf/-inf scores
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf1">>,<<"+inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf2">>,<<"+inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zinf3">>,2,<<"zinf1">>,<<"zinf2">>]),
	<<"inf">> = erldis_client:sr_scall(Client,[<<"zscore">>,<<"zinf3">>,<<"key">>]),
	
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf1">>,<<"+inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf2">>,<<"-inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zinf3">>,2,<<"zinf1">>,<<"zinf2">>]),
	false = erldis_client:sr_scall(Client,[<<"zscore">>,<<"zinf3">>,<<"key">>]),
	
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf1">>,<<"-inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf2">>,<<"+inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zinf3">>,2,<<"zinf1">>,<<"zinf2">>]),
	false = erldis_client:sr_scall(Client,[<<"zscore">>,<<"zinf3">>,<<"key">>]),
	
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf1">>,<<"-inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf2">>,<<"-inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zinf3">>,2,<<"zinf1">>,<<"zinf2">>]),
	<<"-inf">> = erldis_client:sr_scall(Client,[<<"zscore">>,<<"zinf3">>,<<"key">>]),
	%% Against non zset key
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"string">>,<<"zsetb">>]),
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"string">>]),
	%% Against non zset destination - Should work
	4 = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"string">>,2,<<"zseta">>,<<"zsetb">>]),
	[<<"a">>,<<"1">>,<<"b">>,<<"3">>,<<"d">>,<<"3">>,<<"c">>,<<"5">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"string">>,0,-1,<<"withscores">>]),
	%% Syntax error
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,1,<<"zseta">>,<<"zsetb">>]),
	?ERR_KEY_NEEDED = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,<<"zseta">>,1,<<"zsetb">>]),
	?ERR_KEY_NEEDED = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,<<"zseta">>,2]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,2]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,2,3,4]),
	?ERR_WEIGHT_VALUE = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,2,<<"four">>]),
	?ERR_WEIGHT_VALUE = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,<<"two">>,4]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"aggregate">>]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"aggregate">>,<<"average">>]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"aggregate">>,<<"min">>,<<"max">>]),
	?ERR_NEG_LENGTH = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,-1,<<"zseta">>]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zunionstore">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,2]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,-1]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zunionstore">>,<<"zsetc">>,<<"zseta">>]).

zinterstore(Config)->
	{client,Client} = lists:keyfind(client, 1, Config),
	ERR_NUM_ARGS = ?ERR_NUM_ARGS(<<"ZINTERSTORE">>),
	3 =	erldis_client:sr_scall(Client,[<<"zadd">>,<<"zseta">>,1,<<"a">>,2,<<"b">>,3,<<"c">>]),
	3 =	erldis_client:sr_scall(Client,[<<"zadd">>,<<"zsetb">>,1,<<"b">>,2,<<"c">>,3,<<"d">>]),
	%% Basic
	2 =	erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>]),
	[<<"b">>,<<"3">>,<<"c">>,<<"5">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With Weights
	2 =	erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,2,3]),
	[<<"b">>,<<"7">>,<<"c">>,<<"12">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With a regular set and weight	
	3 =	erldis_client:sr_scall(Client,[<<"sadd">>,<<"seta">>,<<"a">>,<<"b">>,<<"c">>]),
	2 =	erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"seta">>,<<"zsetb">>,<<"weights">>,2,3]),
	[<<"b">>,<<"5">>,<<"c">>,<<"8">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With Aggregate MIN
	2 =	erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"aggregate">>,<<"min">>]),
	[<<"b">>,<<"1">>,<<"c">>,<<"2">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With Aggregate MAX
	2 =	erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"aggregate">>,<<"max">>]),
	[<<"b">>,<<"2">>,<<"c">>,<<"3">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With WEIGHTS and AGGREGATE MIN
	2 = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,2,3,<<"aggregate">>,<<"min">>]),
  [<<"b">>,<<"3">>,<<"c">>,<<"6">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	2 = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,1,3,<<"aggregate">>,<<"min">>]),
  [<<"b">>,<<"2">>,<<"c">>,<<"3">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With WEIGHTS and AGGREGATE MAX
	2 = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,2,3,<<"aggregate">>,<<"max">>]),
  [<<"b">>,<<"4">>,<<"c">>,<<"6">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	2 = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,1,3,<<"aggregate">>,<<"max">>]),
  [<<"b">>,<<"3">>,<<"c">>,<<"6">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"zsetc">>,0,-1,<<"withscores">>]),
	%% With +inf/-inf scores
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf1">>,<<"+inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf2">>,<<"+inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zinf3">>,2,<<"zinf1">>,<<"zinf2">>]),
	<<"inf">> = erldis_client:sr_scall(Client,[<<"zscore">>,<<"zinf3">>,<<"key">>]),
	
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf1">>,<<"+inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf2">>,<<"-inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zinf3">>,2,<<"zinf1">>,<<"zinf2">>]),
	false = erldis_client:sr_scall(Client,[<<"zscore">>,<<"zinf3">>,<<"key">>]),
	
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf1">>,<<"-inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf2">>,<<"+inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zinf3">>,2,<<"zinf1">>,<<"zinf2">>]),
	false = erldis_client:sr_scall(Client,[<<"zscore">>,<<"zinf3">>,<<"key">>]),
	
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf1">>,<<"-inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zadd">>,<<"zinf2">>,<<"-inf">>,<<"key">>]),
	true = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zinf3">>,2,<<"zinf1">>,<<"zinf2">>]),
	<<"-inf">> = erldis_client:sr_scall(Client,[<<"zscore">>,<<"zinf3">>,<<"key">>]),
	%% Against non zset key
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"string">>,<<"zsetb">>]),
	?ERR_BAD_KEY = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"string">>]),
	%% Against non zset destination - Should work
	2 = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"string">>,2,<<"zseta">>,<<"zsetb">>]),
	[<<"b">>,<<"3">>,<<"c">>,<<"5">>] =	erldis_client:scall(Client,[<<"zrange">>,<<"string">>,0,-1,<<"withscores">>]),
	%% Syntax error
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,1,<<"zseta">>,<<"zsetb">>]),
	?ERR_KEY_NEEDED = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,<<"zseta">>,1,<<"zsetb">>]),
	?ERR_KEY_NEEDED = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,<<"zseta">>,2]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,2]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,2,3,4]),
	?ERR_WEIGHT_VALUE = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,2,<<"four">>]),
	?ERR_WEIGHT_VALUE = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"weights">>,<<"two">>,4]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"aggregate">>]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"aggregate">>,<<"average">>]),
	?ERR_SYNTAX = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2,<<"zseta">>,<<"zsetb">>,<<"aggregate">>,<<"min">>,<<"max">>]),
	?ERR_NEG_LENGTH = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,-1,<<"zseta">>]),
	%% Wrong numbers of arguments
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zinterstore">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,2]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,-1]),
	ERR_NUM_ARGS = erldis_client:sr_scall(Client,[<<"zinterstore">>,<<"zsetc">>,<<"zseta">>]).