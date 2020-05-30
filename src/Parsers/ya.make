LIBRARY()

PEERDIR(
    clickhouse/src/Common
)

SRCS(
    ASTAlterQuery.cpp
    ASTAsterisk.cpp
    ASTColumnDeclaration.cpp
    ASTColumnsMatcher.cpp
    ASTConstraintDeclaration.cpp
    ASTCreateQuery.cpp
    ASTCreateQuotaQuery.cpp
    ASTCreateRoleQuery.cpp
    ASTCreateRowPolicyQuery.cpp
    ASTCreateSettingsProfileQuery.cpp
    ASTCreateUserQuery.cpp
    ASTDictionary.cpp
    ASTDictionaryAttributeDeclaration.cpp
    ASTDropAccessEntityQuery.cpp
    ASTDropQuery.cpp
    ASTExpressionList.cpp
    ASTFunction.cpp
    ASTFunctionWithKeyValueArguments.cpp
    ASTGrantQuery.cpp
    ASTIdentifier.cpp
    ASTInsertQuery.cpp
    ASTKillQueryQuery.cpp
    ASTLiteral.cpp
    ASTOptimizeQuery.cpp
    ASTOrderByElement.cpp
    ASTPartition.cpp
    ASTQualifiedAsterisk.cpp
    ASTQueryParameter.cpp
    ASTQueryWithOnCluster.cpp
    ASTQueryWithOutput.cpp
    ASTQueryWithTableAndOutput.cpp
    ASTRolesOrUsersSet.cpp
    ASTRowPolicyName.cpp
    ASTSampleRatio.cpp
    ASTSelectQuery.cpp
    ASTSelectWithUnionQuery.cpp
    ASTSetRoleQuery.cpp
    ASTSettingsProfileElement.cpp
    ASTShowAccessEntitiesQuery.cpp
    ASTShowCreateAccessEntityQuery.cpp
    ASTShowGrantsQuery.cpp
    ASTShowPrivilegesQuery.cpp
    ASTShowTablesQuery.cpp
    ASTSubquery.cpp
    ASTSystemQuery.cpp
    ASTTablesInSelectQuery.cpp
    ASTTTLElement.cpp
    ASTUserNameWithHost.cpp
    ASTWithAlias.cpp
    CommonParsers.cpp
    ExpressionElementParsers.cpp
    ExpressionListParsers.cpp
    formatAST.cpp
    IAST.cpp
    iostream_debug_helpers.cpp
    IParserBase.cpp
    Lexer.cpp
    makeASTForLogicalFunction.cpp
    parseDatabaseAndTableName.cpp
    parseIdentifierOrStringLiteral.cpp
    parseIntervalKind.cpp
    parseQuery.cpp
    ParserAlterQuery.cpp
    ParserCase.cpp
    ParserCheckQuery.cpp
    ParserCreateQuery.cpp
    ParserCreateQuotaQuery.cpp
    ParserCreateRoleQuery.cpp
    ParserCreateRowPolicyQuery.cpp
    ParserCreateSettingsProfileQuery.cpp
    ParserCreateUserQuery.cpp
    ParserDescribeTableQuery.cpp
    ParserDictionary.cpp
    ParserDictionaryAttributeDeclaration.cpp
    ParserDropAccessEntityQuery.cpp
    ParserDropQuery.cpp
    ParserGrantQuery.cpp
    ParserInsertQuery.cpp
    ParserKillQueryQuery.cpp
    ParserOptimizeQuery.cpp
    ParserPartition.cpp
    ParserQuery.cpp
    ParserQueryWithOutput.cpp
    ParserRenameQuery.cpp
    ParserRolesOrUsersSet.cpp
    ParserRowPolicyName.cpp
    ParserSampleRatio.cpp
    ParserSelectQuery.cpp
    ParserSelectWithUnionQuery.cpp
    ParserSetQuery.cpp
    ParserSetRoleQuery.cpp
    ParserSettingsProfileElement.cpp
    ParserShowAccessEntitiesQuery.cpp
    ParserShowCreateAccessEntityQuery.cpp
    ParserShowGrantsQuery.cpp
    ParserShowPrivilegesQuery.cpp
    ParserShowTablesQuery.cpp
    ParserSystemQuery.cpp
    ParserTablePropertiesQuery.cpp
    ParserTablesInSelectQuery.cpp
    ParserUnionQueryElement.cpp
    ParserUseQuery.cpp
    ParserUserNameWithHost.cpp
    ParserWatchQuery.cpp
    parseUserName.cpp
    queryToString.cpp
    TokenIterator.cpp
)

END()
