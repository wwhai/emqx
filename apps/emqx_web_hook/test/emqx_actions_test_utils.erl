%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_actions_test_utils).
%% ====================================================================
%% API functions
%% ====================================================================
-export([ create_resource/1
        , delete_resource/1
        , create_rule_with_resource/3
        , create_rule/1
        , delete_rule/1
        , create_resource_failed/1
        , create_rules_failed/1
        , total_records_count/1
        , total_records_count/2
        , clean_start_emulator/1
        , get_rule_matched/1
        , update_action_options/2
        , make_action_options/2
        ]).

-export([ str/1
        , bin/1
        ]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
%%
-define(API_HOST, "http://127.0.0.1:8081/").
-define(API_VERSION, "v4").
-define(BASE_PATH, "api").
%%
%% ====================================================================
%% Http request function
%% ====================================================================

create_resource(SourceOptions) ->
    {ok, R} = request_api(post, api_path(["resources"]), [], auth_header(), SourceOptions),
    % ct:pal("======> Create resource:>~p~nwith options: ~p~n", [R, SourceOptions]),
    0 = get(<<"code">>, R),
    ResourceID = maps:get(<<"id">>, get(<<"data">>, R)),
    ResourceID.

create_rule_with_resource(RuleSQL, #{<<"params">> := Params0} = ActionOptions, ResourceID) ->
    Params = Params0#{<<"$resource">> => ResourceID},
    create_rule(#{<<"rawsql">> => RuleSQL,
                  <<"actions">> => [ActionOptions#{<<"params">> => Params}]}).

create_rule(RulesOptions) ->
    {ok, RuleData} = request_api(post, api_path(["rules"]), [], auth_header(), RulesOptions),
    % ct:pal("======> Create rules:>~p~nwith options: ~p~n", [RuleData, RulesOptions]),
    0 = get(<<"code">>, RuleData),
    RuleID = maps:get(<<"id">>, get(<<"data">>, RuleData)),
    RuleID.

delete_resource(ResourceID) ->
    {ok, _} = request_api(delete, api_path(["resources", binary_to_list(ResourceID)]), auth_header()).

delete_rule(RuleID) ->
    {ok, _} = request_api(delete, api_path(["rules", binary_to_list(RuleID)]), auth_header()).

create_resource_failed(SourceOptions) ->
    {ok, R} = request_api(post, api_path(["resources"]), [], auth_header(), SourceOptions),
    400 = get(<<"code">>, R),
    ct:pal("======> Create resource failed test OK\n").

create_rules_failed(RulesOptions) ->
    {ok, RuleData} = request_api(post, api_path(["rules"]), [], auth_header(), RulesOptions),
    400 = get(<<"code">>, RuleData),
    ct:pal("======> Create rules failed test OK\n").

get_rule_matched(RuleID) ->
    {ok, Data} = request_api(get, api_path(["rules",binary_to_list(RuleID)]), auth_header()),
    [RuleMetric | _ ] = maps:get(<<"metrics">>, get(<<"data">>, Data)),
    [Action | _ ] = maps:get(<<"actions">>, get(<<"data">>, Data)),
    [ActionMetric | _ ] = maps:get(<<"metrics">>, Action, 0),
    #{matched => maps:get(<<"matched">>, RuleMetric),
      success => maps:get(<<"success">>, ActionMetric),
      failed => maps:get(<<"failed">>, ActionMetric)}.

update_action_options(Opts = #{<<"params">> := BaseParams}, Params) ->
    Opts#{<<"params">> => maps:merge(BaseParams, Params)}.

make_action_options(ActName, Params) ->
    #{<<"name">> => ActName,
      <<"params">> => Params}.

clean_start_emulator(EmulatorName) ->
    process_flag(trap_exit, true),
    case EmulatorName:start_link() of
        {error, {already_started, _}} ->
            EmulatorName:stop(),
            {ok, _} = EmulatorName:start_link(),
            ok;
        {ok, _} -> ok
    end.

total_records_count(SqlList) ->
    total_records_count(SqlList, sql).

total_records_count(SqlList, SqlType) ->
    io:format("---- list: ~p", [SqlList]),
    lists:foldl(fun(Sql, Acc) ->
            Acc + records_count(Sql, SqlType)
        end, 0, SqlList).

records_count(Sql, sql) ->
    case emqx_rule_actions_utils:split_insert_sql(str(Sql)) of
        {ok, {_, Values}} ->
            length(re:split(Values, "\\),", [{return, list}]));
        {error, not_insert_sql} ->
            1
    end;

records_count(Sql, cassandra) ->
    case re:split(Sql, "((?i)values)",[{return,binary}]) of
        [_InsertPart | ParamsPart] ->
            length([V || V <- ParamsPart, V =/= <<"values">>]);
        _ -> error(not_cassandra_sql)
    end;

records_count([" values (" | Sql], dolphindb) ->
    Data = [Elm || Elm <- hd(hd(Sql)), Elm =/= "[", Elm =/= "]", Elm =/= ","],
    length(Data);
records_count(Sql, dolphindb) when is_binary(Sql) ->
    1;

records_count("INSERT ALL" ++ Sql, oracle) ->
    case re:run(Sql, "((?i)values)", [global]) of
        {match, Matched} -> length(Matched);
        _ -> error({not_match, Sql})
    end;
records_count("INSERT INTO" ++ _ = Sql, oracle) ->
    records_count(Sql, sql).

%%==============================================================================

request_api(Method, Url, Auth) ->
    % ct:pal("API request, Method=> ~p Url~p~n", [Method, Url]),
    request_api(Method, Url, [], Auth, []).

request_api(Method, Url, QueryParams, Auth, []) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth]});
request_api(Method, Url, QueryParams, Auth, Body) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth], "application/json", emqx_json:encode(Body)}).

do_request_api(Method, Request)->
    % ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return} }
            when Code =:= 200 orelse Code =:= 201 ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header() ->
    AppId = <<"admin">>,
    AppSecret = <<"public">>,
    auth_header(binary_to_list(AppId), binary_to_list(AppSecret)).

auth_header(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User,":",Pass])),
    {"Authorization","Basic " ++ Encoded}.

api_path(Parts)->
    ?API_HOST ++ filename:join([?BASE_PATH, ?API_VERSION] ++ Parts).

get(Key, ResponseBody) ->
    maps:get(Key, jiffy:decode(list_to_binary(ResponseBody), [return_maps])).

str(Bin) when is_binary(Bin) -> binary_to_list(Bin);
str(List) when is_list(List) -> List;
str(Atom) when is_atom(Atom) -> atom_to_list(Atom);
str(Int) when is_integer(Int) -> integer_to_list(Int).

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> iolist_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
bin(Int) when is_integer(Int) -> integer_to_binary(Int).