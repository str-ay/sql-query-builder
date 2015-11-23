import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Simple sql builder for dynamic sql query construct.
 * The most precious benefit: {@link #cascadeChange(Consumer)}
 * Use strings for non implemented functionality
 * {@link SelectBuilder#trustMeAndJustReturnThisString(String)}
 * {@link WhereBuilder#trustMeAndJustReturnThisString(String)}
 *
 * @author Aleksandr Streltsov (jness.pro@gmail.com)
 */
public class SqlBuilder implements Cloneable {

    private static final String LINE_SEPARATOR = "\n ";
    private static final String WITH = "WITH";
    private String queryName;
    private StringBuilder queryBuilder = new StringBuilder();
    private WithQueriesCollection withQueries = new WithQueriesCollection();

    private SelectBuilder selectQueryBuilder = new SelectBuilder();

    public SqlBuilder() {
    }

    public SqlBuilder(SqlBuilder sqlBuilder) {
        this.queryName = sqlBuilder.getQueryName();
        this.queryBuilder = new StringBuilder(sqlBuilder.getQueryBuilder());
        this.withQueries = new WithQueriesCollection(sqlBuilder.withQueries);
        this.selectQueryBuilder = new SelectBuilder(sqlBuilder.select());
    }

    /**
     * Изменить запрос и запросы от него зависимые
     */
    public SqlBuilder cascadeChange(Consumer<SqlBuilder> consumer) {
        consumer.accept(this);
        select().unionQueries.forEach(consumer);
        return this;
    }

    /**
     * Добавить в секцию WITH запрос с проверкой, что он окажется перед другим
     * sqlBuilder - ссылка! Новый SqlBuilder не будет создан, сам создай если нужно
     * new SqlBuilder(sqlBuilder)
     *
     * @param beforeKey имя запроса, перед которым добавить
     * @see #addWith(SqlBuilder)
     */
    public SqlBuilder addWith(SqlBuilder sqlBuilder, String beforeKey) {
        withQueries.add(beforeKey, sqlBuilder);
        return this;
    }

    /**
     * Добавить в секцию WITH запрос
     *
     * @see #addWith(SqlBuilder, String)
     */
    public SqlBuilder addWith(SqlBuilder sqlBuilder) {
        return addWith(sqlBuilder, null);
    }

    /**
     * Коллекция-строитель секции WITH
     */
    public WithQueriesCollection with() {
        return withQueries;
    }

    /**
     * Строитель секции SELECT
     */
    public SelectBuilder select() {
        return selectQueryBuilder;
    }

    public String build() {
        validate();
        queryBuilder = new StringBuilder();

        //<editor-fold desc="WITH">
        if (withQueries.size() > 0) {
            queryBuilder.append(WITH).append(" ");
            for (SqlBuilder sqlBuilder : withQueries) {
                validate(sqlBuilder, true);
                if (queryBuilder.toString().length() != WITH.length() + 1) {
                    queryBuilder.append(", ");
                }

                queryBuilder.append(sqlBuilder.getQueryName())
                        .append(" AS (").append(LINE_SEPARATOR)
                        .append(sqlBuilder.build())
                        .append(LINE_SEPARATOR)
                        .append(")");
            }
            queryBuilder.append(LINE_SEPARATOR);
        }
        //</editor-fold>

        //<editor-fold desc="QUERY">
        queryBuilder.append(selectQueryBuilder.build());
        //</editor-fold>

        return queryBuilder.toString();
    }

    private void validate() {
        validate(this, false);
    }

    /**
     * Валидация
     *
     * @param innerQuery limit infinity. If inner query, then check that non self-referenced object
     */
    private void validate(SqlBuilder sqlBuilder, boolean innerQuery) {
        if (innerQuery && sqlBuilder == this) {
            throw new IllegalStateException("Self referenced link at [WITH] section. QueryName: " + sqlBuilder.getQueryName());
        }
    }

    /**
     * @return builder's clone, but with number mark in {@link SqlBuilder#queryName}
     */
    public SqlBuilder cloneMe() {
        SqlBuilder clone = new SqlBuilder(this);
        if (clone.getQueryName() != null) {
            clone.setQueryName(clone.getQueryName() + " union clone");
        }
        return clone;
    }

    /**
     * Clone current builder and add it as UNION element
     *
     * @return link to added clone
     */
    public SqlBuilder cloneAndUnion() {
        SqlBuilder clone = cloneMe();
        select().addUnion(clone);
        return clone;
    }

    /**
     * remove link for current selectBuilder. Will be alive if link stored elsewhere
     */
    public void resetSelectBuilder() {
        selectQueryBuilder = new SelectBuilder();
    }

    public static class WithQueriesCollection extends LinkedList<SqlBuilder> {

        public WithQueriesCollection() {
        }

        public WithQueriesCollection(Collection<? extends SqlBuilder> c) {
            addAll(c.stream().map(SqlBuilder::new).collect(Collectors.toList()));
        }

        /**
         * Добавить в секцию WITH ссылку на новый запрос
         *
         * @param beforeQueryName перед кем должен стоять запрос по порядку
         */
        public void add(String beforeQueryName, SqlBuilder builder) {
            if (builder != null && get(builder.getQueryName()) == null) {
                if (beforeQueryName != null) {
                    for (int i = 0; i < this.size(); i++) {
                        if (get(i).getQueryName().equals(beforeQueryName)) {
                            add(i, builder);
                            return;
                        }
                    }
                }
                // если не нашли перед кем попросили поставить - поставим в конец
                add(builder);
            }
        }

        public SqlBuilder get(String queryName) {
            for (SqlBuilder sqlBuilder : this) {
                if (sqlBuilder.getQueryName().equals(queryName)) {
                    return sqlBuilder;
                }
            }
            return null;
        }
    }

    public class SelectBuilder {

        private Map<String, String> fields = new LinkedHashMap<>();
        private FromBuilder from = new FromBuilder();
        private WhereBuilder where = new WhereBuilder();
        private GroupByBuilder groupBy = new GroupByBuilder();
        private List<SqlBuilder> unionQueries = new ArrayList<>();
        private OrderByBuilder orderByBuilder = new OrderByBuilder();
        private Integer limit;
        private Integer page;
        private boolean distinct = false;
        private String distinctOn;

        private static final String SELECT = "SELECT";

        /**
         * Warning! Setting this property may crash {@link SqlBuilder},
         * break your dreams, destroy server, kill your dog, burn down your house,
         * cause a nuclear reactor meltdown
         * or otherwise result in all manner of undesirable effects.
         */
        private boolean trustMeAndJustReturnThisString = false;
        private String trustedString;

        public SelectBuilder() {

        }

        public SelectBuilder(SelectBuilder select) {
            this.fields = new LinkedHashMap<>(select.fields);
            this.from = new FromBuilder(select.from);
            this.where = new WhereBuilder(select.where);
            this.groupBy = new GroupByBuilder(select.groupBy);
            this.orderByBuilder = new OrderByBuilder(select.orderByBuilder);
            this.unionQueries.addAll(select.unionQueries.stream().map(SqlBuilder::new).collect(Collectors.toList()));
        }

        public SelectBuilder distinct() {
            this.distinct = true;
            return this;
        }

        public SelectBuilder distinct(boolean dist) {
            this.distinct = dist;
            if (!distinct) {
                distinctOn = null;
            }
            return this;
        }

        public SelectBuilder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public SelectBuilder page(int page) {
            this.page = page;
            return this;
        }

        /**
         * @param distinctOn will be placed here: DISTINCT ON (#distinctOn)
         * @throws IllegalArgumentException distinctOn Can not be empty
         */
        public SelectBuilder distinct(String distinctOn) {
            if (SqlBuilder.isEmpty(distinctOn)) {
                throw new IllegalArgumentException("distinctOn Can not be empty");
            }
            this.distinct = true;
            this.distinctOn = String.format(" ON (%s)", distinctOn);
            return this;
        }

        /**
         * @param field example: count(*) as quantity
         */
        public SelectBuilder addField(String field) {
            return addField(null, field);
        }

        /**
         * @param key   example: rowQuantity
         * @param field example: count(*) as quantity
         */
        public SelectBuilder addField(String key, String field) {
            if (SqlBuilder.isEmpty(field)) {
                throw new IllegalArgumentException("fieldName can not be null");
            } else {
                if (key == null) {
                    key = field;
                }
                if (fields.get(key) == null) {
                    fields.put(key, field);
                }
            }
            return this;
        }

        /**
         * @param key   field key to replace
         * @param field new field
         * @throws NullPointerException if field does not exist
         */
        public SelectBuilder replaceField(String key, String field) {
            if (fields.get(key) != null) {
                fields.replace(key, field);
            } else {
                throw new NullPointerException("Field with key [" + key + "] does not exist");
            }
            return this;
        }

        /**
         * @throws NullPointerException if field does not exist
         */
        public SelectBuilder removeField(String key) {
            if (fields.get(key) != null) {
                fields.remove(key);
            } else {
                throw new NullPointerException("Нет поля по такому ключу [" + key + "]");
            }
            return this;
        }

        public SelectBuilder removeFieldIfExists(String key) {
            if (fields.get(key) != null) {
                fields.remove(key);
            }
            return this;
        }

        /**
         * @return to FROM section
         * @throws IllegalStateException Query must have a name
         */
        public FromBuilder from() {
            return from;
        }

        public SelectBuilder addUnion(SqlBuilder sqlBuilder) {
            for (SqlBuilder unionQuery : unionQueries) {
                if (SqlBuilder.isEmpty(unionQuery.getQueryName())) {
                    throw new IllegalStateException("Query must have a name");
                }
                if (!SqlBuilder.isEmpty(unionQuery.getQueryName()) &&
                        unionQuery.getQueryName().equals(sqlBuilder.getQueryName())) {
                    return this;
                }
            }
            unionQueries.add(sqlBuilder);
            return this;
        }

        /**
         * @return WHERE section
         */
        public WhereBuilder where() {
            return where;
        }

        /**
         * @return GROUP BY section
         */
        public GroupByBuilder groupBy() {
            return groupBy;
        }

        /**
         * @return ORDER BY section
         */
        public OrderByBuilder orderBy() {
            return orderByBuilder;
        }

        /**
         * Warning! Setting this property may crash {@link SqlBuilder},
         * break your dreams, destroy server, kill your dog, burn down your house,
         * cause a nuclear reactor meltdown
         * or otherwise result in all manner of undesirable effects.
         *
         * @param trustedString complete select. Example "select * from users u where u.owner_id = 123 order by u.username"
         * @throws IllegalArgumentException Trusted string can not be empty
         */
        public void trustMeAndJustReturnThisString(String trustedString) {
            if (SqlBuilder.isEmpty(trustedString)) {
                throw new IllegalArgumentException("Trusted string can not be empty");
            }
            trustMeAndJustReturnThisString = true;
            this.trustedString = trustedString;
        }

        /**
         * build SELECT
         *
         * @return [SELECT ...]
         */
        public String build() {
            if (trustMeAndJustReturnThisString) {
                return trustedString;
            }

            StringBuilder builder = new StringBuilder();
            builder.append(SELECT).append(LINE_SEPARATOR);
            if (fields.size() > 0) {
                for (Map.Entry<String, String> fieldEntry : fields.entrySet()) {
                    if (builder.toString().length() > SELECT.length() + LINE_SEPARATOR.length()) {
                        builder.append(", ");
                    } else {
                        if (distinct) {
                            builder.append("DISTINCT");
                            if (distinctOn != null && distinctOn.trim().length() > 0) {
                                builder.append(distinctOn);
                            }
                            builder.append(LINE_SEPARATOR);
                        }
                    }
                    builder.append(fieldEntry.getValue());
                }
            } else {
                builder.append("NULL");
            }

            builder.append(LINE_SEPARATOR).append(from.build()).append(LINE_SEPARATOR)
                    .append(where.build()).append(LINE_SEPARATOR)
                    .append(groupBy.build());

            //<editor-fold desc="UNION">
            if (unionQueries.size() > 0) {
                for (SqlBuilder sqlBuilder : unionQueries) {
                    SqlBuilder.this.validate(sqlBuilder, true);
                    builder.append(LINE_SEPARATOR)
                            .append("UNION").append(LINE_SEPARATOR)
                            .append(sqlBuilder.build());
                }
            }
            //</editor-fold>

            builder.append(orderBy().build());

            if (limit != null) {
                builder.append(LINE_SEPARATOR).append("LIMIT ").append(limit);
                if (page != null && page > 0) {
                    builder.append(" OFFSET ").append(limit * (page - 1));
                }
            }

            return builder.toString();
        }

        public boolean isEmpty() {
            return fields.size() == 0;
        }

        public void removeAllFields() {
            fields = new LinkedHashMap<>();
            distinct = false;
            distinctOn = null;
        }

        public void removeLimit() {
            limit = null;
            page = null;
        }
    }

    /**
     * [FROM ...] section
     */
    public static class FromBuilder {

        private static final String FROM = "FROM";
        private Map<String, Table> tables = new HashMap<>();

        /**
         * Warning! Setting this property may crash {@link SqlBuilder},
         * break your dreams, destroy server, kill your dog, burn down your house,
         * cause a nuclear reactor meltdown
         * or otherwise result in all manner of undesirable effects.
         */
        //todo fatality functionality
        private boolean trustMeAndJustReturnThisString = false;
        private String trustedString;

        public FromBuilder() {
        }

        public FromBuilder(FromBuilder from) {
            for (Map.Entry<String, Table> builders : from.tables.entrySet()) {
                this.tables.put(builders.getKey(), new Table(builders.getValue()));
            }
        }

        /**
         * Warning! Setting this property may crash {@link SqlBuilder},
         * break your dreams, destroy server, kill your dog, burn down your house,
         * cause a nuclear reactor meltdown
         * or otherwise result in all manner of undesirable effects.
         *
         * @param trustedString complete select. Example "select * from users u where u.owner_id = 123 order by u.username"
         * @throws IllegalArgumentException Trusted string can not be empty
         */
        public void trustMeAndJustReturnThisString(String trustedString) {
            if (SqlBuilder.isEmpty(trustedString)) {
                throw new IllegalArgumentException("Trusted string can not be empty");
            }
            trustMeAndJustReturnThisString = true;
            this.trustedString = trustedString;
        }

        /**
         * Warning! Setting this property may crash {@link SqlBuilder},
         * break your dreams, destroy server, kill your dog, burn down your house,
         * cause a nuclear reactor meltdown
         * or otherwise result in all manner of undesirable effects.
         *
         * @param trustedString complete select. Example "select * from users u where u.owner_id = 123 order by u.username"
         * @throws IllegalArgumentException Trusted string can not be empty
         */
        public void trustMeAndJustReturnThisString(StringBuilder trustedString) {
            trustMeAndJustReturnThisString(trustedString.toString());
        }

        /**
         * @param tableData добавляется как ресурс во FROM секции
         * @throws IllegalArgumentException если передал пустое значение или ключ
         */
        public FromBuilder addTable(String key, String tableData) {
            if (SqlBuilder.isEmpty(tableData)) {
                throw new IllegalArgumentException(key + " value is empty");
            } else {
                if (key == null) {
                    key = tableData;
                }
                if (tables.get(key) == null) {
                    tables.put(key, new Table(tableData));
                }
            }
            return this;
        }

        /**
         * @throws IllegalArgumentException если передал пустое значение или ключ
         * @throws IllegalStateException    SqlBuilder name must be set
         */
        public FromBuilder addTable(SqlBuilder queryAsTable) {
            if (queryAsTable == null) {
                throw new IllegalArgumentException("queryAsTable can not be NULL");
            }
            String queryName = queryAsTable.getQueryName();
            if (SqlBuilder.isEmpty(queryName)) {
                throw new IllegalStateException("SqlBuilder name must be set");
            }
            if (tables.get(queryName) == null) {
                tables.put(queryName, new Table(queryAsTable));
            }
            return this;
        }

        /**
         * @param tableData добавляется как ресурс во FROM секции
         * @throws IllegalArgumentException если передал пустое значение или ключ
         */
        public FromBuilder addTable(String tableData) {
            return addTable(null, tableData);
        }

        /**
         * @throws IllegalStateException if join already exists
         */
        public FromBuilder addTableJoin(String tableKey, String joinKey, CharSequence joinData) {
            Table table = tables.get(tableKey);
            if (table != null) {
                if (table.containsJoin(joinKey)) {
                    throw new IllegalStateException(String.format("Join with alias [%s] already exists at table [%s]", joinKey, tableKey));
                }
                table.getJoins().put(joinKey, joinData.toString());
            }
            return this;
        }

        public FromBuilder addJoinToTableIfNotExists(String tableKey, String joinAlias, CharSequence joinData) {
            Table table = tables.get(tableKey);
            if (table != null) {
                if (table.containsJoin(joinAlias)) {
                    return this;
                }
                table.getJoins().put(joinAlias, joinData.toString());
            }
            return this;
        }

        /**
         * @throws NullPointerException
         */
        public FromBuilder removeTable(String tableKey) {
            tables.remove(tableKey);
            return this;
        }

        public String build() {
            if (trustMeAndJustReturnThisString) {
                return trustedString;
            }

            if (tables.size() != 0) {
                StringBuilder builder = new StringBuilder()
                        .append(FROM).append(LINE_SEPARATOR);
                for (Table table : tables.values()) {
                    if (builder.toString().length() > FROM.length() + LINE_SEPARATOR.length()) {
                        builder.append(", ").append(LINE_SEPARATOR);
                    }
                    builder.append(table.build());
                }
                return builder.toString();
            } else {
                return "";
            }
        }

        public FromBuilder replaceTableJoin(String tableKey, String joinKey, String newJoin) {
            Table table = tables.get(tableKey);
            if (table != null) {
                table.replaceJoin(joinKey, newJoin);
            } else {
                throw new NullPointerException(String.format("There is no table with key [%s]", tableKey));
            }
            return this;
        }

        public FromBuilder removeTableJoin(String tableKey, String joinKey) {
            Table table = tables.get(tableKey);
            if (table != null) {
                table.removeJoin(joinKey);
            } else {
                throw new NullPointerException(String.format("There is no table with key [%s]", tableKey));
            }
            return this;
        }

        /**
         * One table with joins
         */
        private class Table {
            private String name;
            private Map<String, String> joins = new LinkedHashMap<>();
            private SqlBuilder sqlBuilder;

            public Table() {
            }

            public Table(Table sourceTable) {
                name = sourceTable.getName();
                joins = new LinkedHashMap<>(sourceTable.getJoins());
            }

            public Table(SqlBuilder source) {
                sqlBuilder = source;
            }

            public Table(String name) {
                this.name = name;
            }

            public StringBuilder build() {
                if (sqlBuilder == null) {
                    StringBuilder stringBuilder = new StringBuilder(name);
                    for (String join : joins.values()) {
                        stringBuilder.append(" \n").append(join);
                    }
                    return stringBuilder;
                } else {
                    return new StringBuilder("(" + sqlBuilder.build() + ") AS " + sqlBuilder.queryName);
                }
            }

            public boolean containsJoin(String joinKey) {
                return joins.get(joinKey) != null;
            }

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public Map<String, String> getJoins() {
                return joins;
            }

            public void setJoins(Map<String, String> joins) {
                this.joins = joins;
            }

            public void replaceJoin(String joinKey, String newJoin) {
                joins.replace(joinKey, newJoin);
            }

            public void removeJoin(String joinKey) {
                joins.remove(joinKey);
            }
        }
    }

    /**
     * GROUP BY ...
     */
    public static class GroupByBuilder {

        private static final String GROUP_BY = "GROUP BY";
        private Map<String, String> groupFields = new LinkedHashMap<>();

        public GroupByBuilder() {
        }

        public GroupByBuilder(GroupByBuilder groupBy) {
            this.groupFields = new HashMap<>(groupBy.groupFields);
        }

        public GroupByBuilder addField(String groupByElement) {
            return addField(null, groupByElement);
        }

        @SuppressWarnings("Duplicates")
        public GroupByBuilder addField(String key, String groupByElement) {
            if (isEmpty(groupByElement)) {
                throw new IllegalArgumentException(key + " value is empty");
            } else {
                if (key == null) {
                    key = groupByElement;
                }
                if (groupFields.get(key) == null) {
                    groupFields.put(key, groupByElement);
                }
            }
            return this;
        }

        public String build() {
            if (groupFields.size() != 0) {
                StringBuilder builder = new StringBuilder()
                        .append(GROUP_BY).append(LINE_SEPARATOR);
                for (String groupField : groupFields.values()) {
                    if (builder.toString().length() > GROUP_BY.length() + LINE_SEPARATOR.length()) {
                        builder.append(", ");
                    }
                    builder.append(groupField);
                }
                return builder.append(" \n").toString();
            } else {
                return "";
            }
        }

        /**
         * @throws NullPointerException
         */
        public void removeGroupElement(String key) {
            groupFields.remove(key);
        }
    }

    /**
     * ORDER BY ...
     */
    public static class OrderByBuilder {

        private static final String ORDER_BY = "ORDER BY";
        private Map<String, String> orderFields = new LinkedHashMap<>();

        public OrderByBuilder() {
        }

        public OrderByBuilder(OrderByBuilder builder) {
            this.orderFields = new HashMap<>(builder.orderFields);
        }

        public OrderByBuilder addField(String orderByElement) {
            return addField(null, orderByElement);
        }

        @SuppressWarnings("Duplicates")
        public OrderByBuilder addField(String key, String orderByElement) {
            if (isEmpty(orderByElement)) {
                throw new IllegalArgumentException(key + " value is empty");
            } else {
                if (key == null) {
                    key = orderByElement;
                }
                if (orderFields.get(key) == null) {
                    orderFields.put(key, orderByElement);
                }
            }
            return this;
        }

        public String build() {
            if (orderFields.size() != 0) {
                StringBuilder builder = new StringBuilder()
                        .append(ORDER_BY).append(LINE_SEPARATOR);

                orderFields.values().stream().forEach(
                        field -> {
                            if (builder.toString().length() > ORDER_BY.length() + LINE_SEPARATOR.length()) {
                                builder.append(", ");
                            }
                            builder.append(field);
                        }
                );
                return builder.append(" \n").toString();
            } else {
                return "";
            }
        }

        /**
         * @throws NullPointerException
         */
        public void removeOrderElement(String key) {
            orderFields.remove(key);
        }

        public void removeAll() {
            orderFields = new LinkedHashMap<>();
        }
    }

    /**
     * WHERE ...
     * <p>
     * Simple AND и OR. No wat group as AND (... OR ...)
     * Use {@link WhereBuilder#andCondition(String)} with not only AND elements
     * or use  {@link WhereBuilder#trustMeAndJustReturnThisString(String)}
     */
    public static class WhereBuilder {

        private static final String WHERE = "WHERE";
        private Map<String, String> andConditions = new HashMap<>();
        private Map<String, String> orConditions = new HashMap<>();

        /**
         * Warning! Setting this property may crash {@link SqlBuilder},
         * break your dreams, destroy server, kill your dog, burn down your house,
         * cause a nuclear reactor meltdown
         * or otherwise result in all manner of undesirable effects.
         */
        private boolean trustMeAndJustReturnThisString = false;
        private String trustedString;

        /**
         * Warning! Setting this property may crash {@link SqlBuilder},
         * break your dreams, destroy server, kill your dog, burn down your house,
         * cause a nuclear reactor meltdown
         * or otherwise result in all manner of undesirable effects.
         */
        private StringBuilder appendix = new StringBuilder();

        public WhereBuilder() {
        }

        public WhereBuilder(WhereBuilder where) {
            this.andConditions = new HashMap<>(where.andConditions);
            this.orConditions = new HashMap<>(where.orConditions);
        }

        /**
         * WHERE ... AND #condition
         * WHERE #condition
         */
        public WhereBuilder andCondition(String condition) {
            return andCondition(null, condition);
        }

        /**
         * WHERE ... AND #condition
         * WHERE #condition
         */
        public WhereBuilder andCondition(String condition, boolean add) {
            return andCondition(null, condition);
        }

        /**
         * WHERE ... AND #condition
         * WHERE #condition
         */
        public WhereBuilder andCondition(String key, String condition) {
            if (isEmpty(condition)) {
                throw new IllegalArgumentException(key + " value is empty");
            } else {
                if (key == null) {
                    key = condition;
                }
                andConditions.put(key, condition);
            }
            return this;
        }

        /**
         * WHERE ... OR #condition
         * WHERE #condition
         */
        public WhereBuilder orCondition(String condition) {
            return orCondition(null, condition);
        }

        /**
         * WHERE ... OR #condition
         * WHERE #condition
         */
        public WhereBuilder orCondition(String key, String condition) {
            if (SqlBuilder.isEmpty(condition)) {
                throw new IllegalArgumentException(key + " value is empty");
            } else {
                if (key == null) {
                    key = condition;
                }
                orConditions.put(key, condition);
            }
            return this;
        }

        /**
         * Warning! Setting this property may crash {@link SqlBuilder},
         * break your dreams, destroy clipper, kill your dog, burn down your house,
         * cause a nuclear reactor meltdown
         * or otherwise result in all manner of undesirable effects.
         *
         * @param trustedString example: "where questions.toBeOrNotToBe = true"
         * @throws IllegalArgumentException Trusted string can not be empty
         */
        public void trustMeAndJustReturnThisString(String trustedString) {
            if (SqlBuilder.isEmpty(trustedString)) {
                throw new IllegalArgumentException("Trusted string can not be empty");
            }
            trustMeAndJustReturnThisString = true;
            this.trustedString = trustedString;
        }

        public String build() {
            if (trustMeAndJustReturnThisString) {
                return trustedString;
            }

            if (andConditions.size() != 0 || orConditions.size() != 0) {
                StringBuilder builder = new StringBuilder()
                        .append(WHERE).append(LINE_SEPARATOR);
                for (String condition : andConditions.values()) {
                    if (builder.toString().length() > WHERE.length() + LINE_SEPARATOR.length()) {
                        builder.append(" AND ");
                    }
                    builder.append(condition);
                }

                for (String condition : orConditions.values()) {
                    if (builder.toString().length() > WHERE.length() + LINE_SEPARATOR.length()) {
                        builder.append(" OR ");
                    }
                    builder.append(condition);
                }
                if (SqlBuilder.isEmpty(appendix)) {
                    builder.append(" \n").append(appendix).append(" \n");
                }
                return builder.toString();
            } else {
                return "";
            }
        }

        public WhereBuilder appendToTheEnd(String appendix) {
            if (!SqlBuilder.isEmpty(appendix)) {
                this.appendix.append(appendix);
            }
            return this;
        }

        public WhereBuilder appendToTheEnd(StringBuilder appendix) {
            return appendToTheEnd(appendix.toString());
        }

        /**
         * @throws NullPointerException
         */
        public WhereBuilder removeAndCondition(String andConditionKey) {
            if (!SqlBuilder.isEmpty(andConditionKey)) {
                andConditions.remove(andConditionKey);
            }
            return this;
        }

        /**
         * @throws NullPointerException
         */
        public WhereBuilder removeOrCondition(String orConditionKey) {
            if (!SqlBuilder.isEmpty(orConditionKey)) {
                orConditions.remove(orConditionKey);
            }
            return this;
        }

        /**
         * @throws NullPointerException
         */
        public WhereBuilder replaceAndCondition(String key, String condition) {
            if (!SqlBuilder.isEmpty(condition)) {
                andConditions.replace(key, condition);
            }
            return this;
        }

        /**
         * @throws NullPointerException
         */
        public WhereBuilder replaceOrCondition(String key, String condition) {
            if (!SqlBuilder.isEmpty(condition)) {
                orConditions.replace(key, condition);
            }
            return this;
        }

        public WhereBuilder removeAllConditions() {
            andConditions = new HashMap<>();
            orConditions = new HashMap<>();
            return this;
        }
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public String getQueryName() {
        return queryName;
    }

    public StringBuilder getQueryBuilder() {
        return queryBuilder;
    }

    private static boolean isEmpty(CharSequence charSequence) {
        return charSequence == null || charSequence.length() == 0;
    }
}
