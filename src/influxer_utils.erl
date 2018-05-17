-module(influxer_utils).

-include_lib("buoy/include/buoy.hrl").

-export([
    start_pool/1
%    ,start_batcher
    ,get_influx_write_url/1
    ,get_auth_token/2
    ,write/6
    ,tag_to_bin/2
    ,to_bin/1
    ,to_list/1
    ,get_read_url/1
]).


start_pool(L) when is_list(L) ->
    lists:foreach(fun(U)-> start_pool(U) end, L);
start_pool(Url) when is_binary(Url) ->
    start_pool({Url, [{pool_size, 10}]});
start_pool({Url, Options}) ->
    BUrl = buoy_utils:parse_url(Url),
    buoy_pool:start(BUrl, Options).

-spec write(buoy_url(), binary(), binary(), [{binary(),binary()}], [{binary(), any()}], integer()) -> any().
write(BUrl, AuthToken, Table, Tags, Values, TimeOut) ->
    Headers   = [{<<"Authorization">>, AuthToken},
                 {<<"Content-Type">>, <<"application/octet-stream">>}],
    TagsBin   = lists:foldl(fun tag_to_bin/2, <<>>, Tags),
    ValuesBin = lists:foldl(fun tag_to_bin/2, <<>>, Values),
    Body      = <<Table/binary,",", TagsBin/binary, " ", ValuesBin/binary, "\n">>, 
    buoy:post(BUrl, Headers, Body, TimeOut).

-spec get_influx_write_url(#{_ => _}) -> buoy_url().
get_influx_write_url(Config) ->
    Host  = to_bin(maps:get(host, Config)),
    Port  = to_bin(maps:get(port, Config)),
    Proto = to_bin(maps:get(proto, Config, <<"http">>)),
    DB    = to_bin(maps:get(db, Config)),
    buoy_utils:parse_url(<<Proto/binary, "://", Host/binary, ":", Port/binary, "/write?db=", DB/binary>>).

-spec get_auth_token(binary(), binary()) -> binary().
get_auth_token(User, Password) when is_binary(User), is_binary(Password) ->
    UP = base64:encode(<<User/binary, ":", Password/binary>>),
    <<"Basic ", UP/binary>>.

tag_to_bin({K,V}, <<>>) when is_binary(K) -> 
    Vv = to_bin(V),
    <<K/binary, "=", Vv/binary>>;
tag_to_bin({K,V}, A) when is_binary(K) -> 
    Vv = to_bin(V),
    <<A/binary, ",", K/binary, "=", Vv/binary>>.

to_bin(F) when is_float(F)   -> float_to_binary(F, [{decimals, 8}, compact]);
to_bin(I) when is_integer(I) -> integer_to_binary(I);
to_bin(L) when is_list(L)    -> list_to_binary(L);
to_bin(B) -> B.

to_list(B) when is_binary(B)  -> binary_to_list(B);
to_list(I) when is_integer(I) -> integer_to_list(I);
to_list(F) when is_float(F)   -> float_to_list(F, [{decimals, 8}, compact]);
to_list(L) -> L.

-spec get_read_url(binary()) -> binary().
get_read_url(Url) when is_binary(Url) ->
    case re:run(Url, "^(https*://[0-9.:]+/).*$") of
        {match, [_, {0, N}]} -> <<U1:N/binary, _/binary>> = Url, <<U1/binary, "query">>
        ;_ -> Url
    end.

