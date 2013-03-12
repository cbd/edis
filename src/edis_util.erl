%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc edis utilities
%%% @end
%%%-------------------------------------------------------------------
-module(edis_util).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-export([timestamp/0, now/0, upper/1, lower/1, binary_to_integer/1, binary_to_integer/2,
         integer_to_binary/1, binary_to_float/1, binary_to_float/2, reverse_tuple_to_list/1,
				 make_pairs/1, glob_to_re/1,random_binary/0, join/2, load_config/1,
				 multiply/2, sum/2, min/2, max/2]).

-include("edis.hrl").

-define(EPOCH, 62167219200).

%% @doc Current timestamp
-spec timestamp() -> float().
timestamp() ->
  ?MODULE:now() + element(3, erlang:now()) / 1000000.

%% @doc UTC in *NIX seconds
-spec now() -> pos_integer().
now() ->
  calendar:datetime_to_gregorian_seconds(calendar:universal_time()) - ?EPOCH.

%% @doc converts all characters in the specified binary to uppercase. 
-spec upper(binary()) -> binary().
upper(Bin) ->
  upper(Bin, <<>>).

%% @private
upper(<<>>, Acc) ->
  Acc;
upper(<<C, Rest/binary>>, Acc) when $a =< C, C =< $z ->
  upper(Rest, <<Acc/binary, (C-32)>>);
upper(<<195, C, Rest/binary>>, Acc) when 160 =< C, C =< 182 -> %% A-0 with tildes plus enye
  upper(Rest, <<Acc/binary, 195, (C-32)>>);
upper(<<195, C, Rest/binary>>, Acc) when 184 =< C, C =< 190 -> %% U and Y with tilde plus greeks
  upper(Rest, <<Acc/binary, 195, (C-32)>>);
upper(<<C, Rest/binary>>, Acc) ->
  upper(Rest, <<Acc/binary, C>>).

%% @doc converts all characters in the specified binary to lowercase
-spec lower(binary()) -> binary().
lower(Bin) ->
  lower(Bin, <<>>).

lower(<<>>, Acc) ->
  Acc;
lower(<<C, Rest/binary>>, Acc) when $A =< C, C =< $Z ->
  lower(Rest, <<Acc/binary, (C+32)>>);
lower(<<195, C, Rest/binary>>, Acc) when 128 =< C, C =< 150 -> %% A-0 with tildes plus enye
  lower(Rest, <<Acc/binary, 195, (C+32)>>);
lower(<<195, C, Rest/binary>>, Acc) when 152 =< C, C =< 158 -> %% U and Y with tilde plus greeks
  lower(Rest, <<Acc/binary, 195, (C+32)>>);
lower(<<C, Rest/binary>>, Acc) ->
  lower(Rest, <<Acc/binary, C>>).

%% @doc returns an integer whose binary representation is Bin
-spec binary_to_integer(binary()) -> integer().
binary_to_integer(Bin) ->
  try list_to_integer(binary_to_list(Bin))
  catch
    _:badarg ->
      throw(not_integer)
  end.

%% @doc returns a float whose binary representation is Bin
-spec binary_to_float(binary()) -> float().
binary_to_float(Bin) ->
		case lower(Bin) of
				<<"inf">>  -> ?POS_INFINITY;
				<<"infinity">>  -> ?POS_INFINITY;
				<<"+infinity">>  -> ?POS_INFINITY;
				<<"+inf">> -> ?POS_INFINITY;
				<<"-inf">> -> ?NEG_INFINITY;
				<<"-infinity">>  -> ?NEG_INFINITY;
				_ ->
						try list_to_float(binary_to_list(Bin))
						catch
								_:badarg ->
										try 1.0 * list_to_integer(binary_to_list(Bin))
										catch
												_:badarg ->
														throw(not_float)
										end
						end
		end.

%% @doc returns an integer whose binary representation is Bin.
%% If Bin is not a integer, Default is returned
-spec binary_to_integer(binary(), integer()) -> integer().
binary_to_integer(Bin, Default) ->
  try list_to_integer(binary_to_list(Bin))
  catch
    _:badarg ->
      try erlang:trunc(list_to_float(binary_to_list(Bin)))
      catch
        _:badarg ->
          lager:warning("Using ~p because we received '~s'. This behaviour was copied from redis-server~n", [Default, Bin]),
          Default
      end
  end.

%% @doc returns a float whose binary representation is Bin.
%% If Bin is not a float, Default is returned
-spec binary_to_float(binary(), float() | undefined) -> float() | undefined.
binary_to_float(Bin, Default) ->
  try list_to_float(binary_to_list(Bin))
  catch
    _:badarg ->
      try 1.0 * list_to_integer(binary_to_list(Bin))
      catch
        _:badarg ->
          Default
      end
  end.

%% @doc returns a binary whose integer representation is Int
-spec integer_to_binary(binary()) -> integer().
integer_to_binary(Int) ->
  list_to_binary(integer_to_list(Int)).

-spec reverse_tuple_to_list({any(),any()}) -> [any()].
reverse_tuple_to_list({F,S}) ->
		[S,F].

%% @doc returns a list of binary tuples. The first tuple contains the first pair of elements in the received list,
%% the second tuple contains the second pair and so on. 
%% If the received list is odd, the last element will be ignored
-spec make_pairs([any()]) -> [{any(), any()}].
make_pairs(KVs) ->
  make_pairs(KVs, []).

make_pairs([], Acc) -> lists:reverse(Acc);
make_pairs([_], Acc) -> lists:reverse(Acc);
make_pairs([K, V | Rest], Acc) ->
  make_pairs(Rest, [{K,V} | Acc]).

%% @doc converts the GLOB into reg exp 
-spec glob_to_re(binary()) -> binary().
glob_to_re(Pattern) ->
  binary:replace(
    binary:replace(
      binary:replace(
        binary:replace(Pattern, <<"*">>, <<".*">>, [global]),
        <<"?">>, <<".">>, [global]),
      <<"(">>, <<"\\(">>, [global]),
    <<")">>, <<"\\)">>, [global]).

%% @doc returns a random binary
-spec random_binary() -> binary().
random_binary() ->
  Now = {_, _, Micro} = erlang:now(),
  Nowish = calendar:now_to_universal_time(Now),
  Nowsecs = calendar:datetime_to_gregorian_seconds(Nowish),
  Then = calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}),
  Prefix = io_lib:format("~14.16.0b", [(Nowsecs - Then) * 1000000 + Micro]),
  list_to_binary(Prefix ++ integer_to_list(Micro) ++ base64:encode(crypto:rand_bytes(9))).

%% @doc joins the list of binaries with Sep
-spec join([binary()], binary()) -> binary().
join([], _) -> <<>>;
join([Bin], _) -> Bin;
join([Bin|Bins], Sep) -> join(Bins, Sep, Bin).

join([], _, Acc) -> Acc;
join([Bin|Bins], Sep, Acc) -> join(Bins, Sep, <<Acc/binary, Sep/binary, Bin/binary>>).

-spec multiply(float(),float()) -> float().
multiply(0.0,_) -> 0.0;
multiply(_,0.0) -> 0.0;
multiply(?POS_INFINITY,V) when V > 0.0 -> ?POS_INFINITY;
multiply(?POS_INFINITY,V) when V < 0.0 -> ?NEG_INFINITY;
multiply(?NEG_INFINITY,V) when V > 0.0 -> ?NEG_INFINITY;
multiply(?NEG_INFINITY,V) when V < 0.0 -> ?POS_INFINITY;
multiply(V,?POS_INFINITY) when V > 0.0 -> ?POS_INFINITY;
multiply(V,?POS_INFINITY) when V < 0.0 -> ?NEG_INFINITY;
multiply(V,?NEG_INFINITY) when V > 0.0 -> ?NEG_INFINITY;
multiply(V,?NEG_INFINITY) when V < 0.0 -> ?POS_INFINITY;
multiply(V1,V2) -> V1*V2.

-spec sum(float(),float()) -> float().
sum(?POS_INFINITY,?NEG_INFINITY) -> 0.0;
sum(?NEG_INFINITY,?POS_INFINITY) -> 0.0;
sum(?POS_INFINITY,_) -> ?POS_INFINITY;
sum(?NEG_INFINITY,_) -> ?NEG_INFINITY;
sum(_,?POS_INFINITY) -> ?POS_INFINITY;
sum(_,?NEG_INFINITY) -> ?NEG_INFINITY;
sum(V1,V2) when V1/V2 < 0.0 -> V1+V2;
sum(V1,V2) when V1 > 0.0, ?POS_INFINITY-V1 < V2 -> ?POS_INFINITY;
sum(V1,V2) when V1 < 0.0, ?NEG_INFINITY-V1 > V2 -> ?NEG_INFINITY;
sum(V1,V2) -> V1+V2.

-spec min(float(),float()) -> float().
min(?POS_INFINITY,V) -> V;
min(V,?POS_INFINITY) -> V;
min(?NEG_INFINITY,_) -> ?NEG_INFINITY;
min(_,?NEG_INFINITY) -> ?NEG_INFINITY;
min(V1,V2) -> erlang:min(V1,V2).

-spec max(float(),float()) -> float().
max(?POS_INFINITY,_) -> ?POS_INFINITY;
max(_,?POS_INFINITY) -> ?POS_INFINITY;
max(?NEG_INFINITY,V) -> V;
max(V,?NEG_INFINITY) -> V;
max(V1,V2) -> erlang:max(V1,V2).

%% @doc Loads an Erlang config file and sets the corresponding application environment variables
-spec load_config(string()) -> ok.
load_config(File) ->
  case file:consult(File) of
    {error, Reason} ->
	  throw(Reason);
    {ok, [Configs]} ->
      lists:foreach(fun load_app_config/1, Configs)
  end.

%% @doc Reads an erlang config file and sets the corresponding application environments
load_app_config({App, Envs}) ->
  case application:load(App) of
    ok -> ok;
    {error, {already_loaded, App}} ->
      ok =
          case application:stop(App) of
            ok -> ok;
            {error, {not_started, App}} -> ok
          end,
      ok = application:unload(App),
      ok = application:load(App)
  end,
  lists:foreach(fun({Key, Value}) ->
                        ok = application:set_env(App, Key, Value)
                end, Envs).