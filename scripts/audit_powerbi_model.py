"""Read-only PostgreSQL schema audit for the Power BI resource model."""

import json
import os
import re
from pathlib import Path

import psycopg2


def fetch_all(cursor, sql):
    cursor.execute(sql)
    columns = [item.name for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def main():
    connection = psycopg2.connect(
        host=os.getenv("AUDIT_PG_HOST", "localhost"),
        port=int(os.getenv("AUDIT_PG_PORT", "5432")),
        user=os.getenv("AUDIT_PG_USER", "hgmedia"),
        password=os.environ["AUDIT_PG_PASSWORD"],
        dbname=os.getenv("AUDIT_PG_DATABASE", "hgmediadb"),
        connect_timeout=10,
        application_name="powerbi_model_readonly_audit",
    )
    connection.set_session(readonly=True, autocommit=True)
    cursor = connection.cursor()

    if os.getenv("AUDIT_RESOURCE_COVERAGE") == "1":
        cursor.execute("set statement_timeout = '120s'")
        coverage = fetch_all(
            cursor,
            """
            with stock as (
                select distinct upper(trim(hg_stock_id)) as resource_id
                from silver.dim_stock
                where nullif(trim(hg_stock_id), '') is not null
            ), editing as (
                select distinct upper(trim(hg_stock_id)) as resource_id
                from silver.fact_editing
                where nullif(trim(hg_stock_id), '') is not null
            ), editing_missing_raw as (
                select e.resource_id
                from editing e
                left join stock s using (resource_id)
                where s.resource_id is null
            ), editing_clean as (
                select distinct regexp_replace(resource_id, '[^0-9A-Z]', '', 'g') as resource_id
                from editing_missing_raw
            ), resources as (
                select distinct upper(trim(hg_stock_id)) as resource_id
                from silver.dim_resources
                where nullif(trim(hg_stock_id), '') is not null
            ), population as (
                select resource_id from stock
                union
                select resource_id from editing
            )
            select
                (select count(*) from stock) as stock_resources,
                (select count(*) from editing) as editing_resources,
                (select count(*) from stock s join editing e using (resource_id)) as overlap_resources,
                (select count(*) from editing e left join stock s using (resource_id)
                  where s.resource_id is null) as editing_missing_from_stock,
                (select count(*) from stock s left join editing e using (resource_id)
                  where e.resource_id is null) as stock_never_edited,
                (select count(*) from population) as union_resources,
                (select count(*) from population p left join resources r using (resource_id)
                  where r.resource_id is null) as union_missing_dim_resources,
                (select count(*) from population where resource_id !~ '^HGFA[0-9A-F]+$')
                    as invalid_hg_codes,
                (select count(*) from editing_clean e left join stock s using (resource_id)
                  where s.resource_id is null) as normalized_editing_missing_from_stock
            """,
        )[0]
        coverage["editing_only_examples"] = fetch_all(
            cursor,
            """
            with missing as (
                select upper(trim(e.hg_stock_id)) as resource_id,
                       count(*) as editing_rows,
                       min(e.position) as first_position
                from silver.fact_editing e
                left join silver.dim_stock s
                  on upper(trim(e.hg_stock_id)) = upper(trim(s.hg_stock_id))
                where nullif(trim(e.hg_stock_id), '') is not null
                  and s.hg_stock_id is null
                group by upper(trim(e.hg_stock_id))
            )
            select m.resource_id,
                   regexp_replace(m.resource_id, '[^0-9A-Z]', '', 'g') as normalized_resource_id,
                   bool_or(s2.hg_stock_id is not null) as normalized_exists_in_stock,
                   m.editing_rows,
                   m.first_position
            from missing m
            left join silver.dim_stock s2
              on regexp_replace(m.resource_id, '[^0-9A-Z]', '', 'g')
               = upper(trim(s2.hg_stock_id))
            group by m.resource_id, m.editing_rows, m.first_position
            order by m.resource_id
            """,
        )
        print(json.dumps(coverage, ensure_ascii=False, default=str, indent=2))
        cursor.close()
        connection.close()
        return

    if os.getenv("AUDIT_FACTS") == "1":
        facts = {
            "fact_columns": fetch_all(
                cursor,
                """
                select c.table_name,
                       string_agg(c.column_name || ':' || c.udt_name, ', ' order by c.ordinal_position) as columns,
                       pc.reltuples::bigint as estimated_rows,
                       pg_total_relation_size(pc.oid) as total_bytes
                from information_schema.columns c
                join pg_namespace pn on pn.nspname = c.table_schema
                join pg_class pc on pc.relnamespace = pn.oid and pc.relname = c.table_name
                where c.table_schema = 'silver' and c.table_name like 'fact\\_%' escape '\\'
                group by c.table_name, pc.reltuples, pc.oid
                order by c.table_name
                """,
            )
        }
        print(json.dumps(facts, ensure_ascii=False, default=str, indent=2))
        cursor.close()
        connection.close()
        return

    if os.getenv("AUDIT_VALIDATE_OBT") == "1" or os.getenv("AUDIT_RUN_OBT") == "1":
        model_name = os.getenv("AUDIT_MODEL", "mart_powerbi_resource_obt")
        model_path = (
            Path(__file__).resolve().parents[1]
            / "dwh_dbt"
            / "models"
            / "gold"
            / f"{model_name}.sql"
        )
        sql = model_path.read_text(encoding="utf-8")
        sql = re.sub(r"\{\{\s*config\([\s\S]*?\)\s*\}\}", "", sql, count=1)
        gold_models = {
            "mart_resource_usage",
            "mart_resource_channel",
            "resources_useage_number",
            "mart_powerbi_resource_obt",
            "mart_video_performance_monthly",
            "mart_repository_usage",
            "mart_team_usage",
            "fact_hao_hut",
        }

        def resolve_ref(match):
            name = match.group(1)
            schema = "gold" if name in gold_models else "silver"
            return f'{schema}."{name}"'

        sql = re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", resolve_ref, sql)
        if os.getenv("AUDIT_RUN_OBT") == "1":
            cursor.execute("set statement_timeout = '120s'")
            if model_name == "fact_powerbi_unified":
                cursor.execute(
                    "select count(*) as rows, count(distinct metric_name) as metrics, "
                    "count(distinct grain_type) as grains, min(metric_date) as min_date, "
                    "max(metric_date) as max_date, "
                    "sum(metric_value) filter (where metric_name = 'resource_count') as resources, "
                    "sum(metric_value) filter (where metric_name = 'used_resource_count_all_links') as used_resources, "
                    "sum(metric_value) filter (where metric_name = 'cold_resource_count') as cold_resources, "
                    "sum(metric_value) filter (where metric_name = 'inventory_resource_count') as inventory_resources "
                    "from (" + sql + ") unified_fact"
                )
            else:
                cursor.execute(
                    "select count(*) as rows, count(distinct resource_id) as distinct_resources, "
                    "sum(resource_count) as resource_count, "
                    "sum(published_resource_video_uses) as published_uses, "
                    "sum(lifetime_views) as lifetime_views, "
                    "sum(lifetime_revenue) as lifetime_revenue from (" + sql + ") obt"
                )
            columns = [item.name for item in cursor.description]
            print(json.dumps(dict(zip(columns, cursor.fetchone())), default=str, indent=2))
        else:
            cursor.execute("set statement_timeout = '15s'")
            cursor.execute("explain " + sql)
            plan = [row[0] for row in cursor.fetchall()]
            print(json.dumps({"parsed": True, "plan": plan}, indent=2))
        cursor.close()
        connection.close()
        return

    if os.getenv("AUDIT_SUMMARY") == "1":
        summary = {
            "schema_totals": fetch_all(
                cursor,
                """
                select n.nspname as schema_name,
                       count(*) as relation_count,
                       sum(pg_total_relation_size(c.oid)) as total_bytes,
                       sum(greatest(c.reltuples, 0))::bigint as estimated_rows
                from pg_class c
                join pg_namespace n on n.oid = c.relnamespace
                where n.nspname in ('silver', 'gold')
                  and c.relkind in ('r', 'p', 'v', 'm')
                group by n.nspname
                order by n.nspname
                """,
            ),
            "gold_columns": fetch_all(
                cursor,
                """
                select table_name,
                       string_agg(column_name || ':' || udt_name, ', ' order by ordinal_position) as columns
                from information_schema.columns
                where table_schema = 'gold'
                group by table_name
                order by table_name
                """,
            ),
            "key_profiles": [],
        }
        profiles = {
            "silver.dim_stock": "select count(*) rows, count(distinct nullif(trim(hg_stock_id), '')) distinct_resource, count(*) filter (where nullif(trim(hg_stock_id), '') is null) blank_resource from silver.dim_stock",
            "silver.dim_resources": "select count(*) rows, count(distinct nullif(trim(hg_stock_id), '')) distinct_resource, count(distinct repository_id) distinct_repository, count(*) filter (where nullif(trim(hg_stock_id), '') is null) blank_resource from silver.dim_resources",
            "gold.resources_useage_number": "select count(*) rows, count(distinct resource_video_sk) distinct_sk, count(distinct hg_stock_id) distinct_resource, count(distinct video_id) distinct_video from gold.resources_useage_number",
            "gold.mart_resource_usage": "select count(*) rows, count(distinct mart_resource_usage_sk) distinct_sk, count(distinct resource_id) distinct_resource, sum(used_video_count) total_video_uses from gold.mart_resource_usage",
            "gold.mart_resource_channel": "select count(*) rows, count(distinct mart_resource_channel_sk) distinct_sk, count(distinct resource_id) distinct_resource, sum(view) total_views, sum(revenue) total_revenue from gold.mart_resource_channel",
            "gold.mart_repository_usage": "select count(*) rows, count(distinct repository_id) distinct_repository, sum(number_resources) resources, sum(used_resources) used_resources, sum(total_resource_usage_published) usage_events from gold.mart_repository_usage",
        }
        cursor.execute("set statement_timeout = '15s'")
        for relation, sql in profiles.items():
            try:
                row = fetch_all(cursor, sql)[0]
                row["relation"] = relation
                summary["key_profiles"].append(row)
            except psycopg2.errors.QueryCanceled:
                summary["key_profiles"].append({"relation": relation, "error": "statement timeout"})
        print(json.dumps(summary, ensure_ascii=False, default=str, indent=2))
        cursor.close()
        connection.close()
        return

    result = {
        "connection": fetch_all(
            cursor,
            "select current_database() as database, current_user as username, version() as version",
        ),
        "relations": fetch_all(
            cursor,
            """
            select n.nspname as schema_name,
                   c.relname as relation_name,
                   case c.relkind when 'r' then 'table' when 'p' then 'partitioned_table'
                        when 'v' then 'view' when 'm' then 'materialized_view' end as relation_type,
                   pg_total_relation_size(c.oid) as total_bytes,
                   c.reltuples::bigint as estimated_rows
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname in ('silver', 'gold')
              and c.relkind in ('r', 'p', 'v', 'm')
            order by n.nspname, c.relname
            """,
        ),
        "columns": fetch_all(
            cursor,
            """
            select table_schema as schema_name, table_name, ordinal_position,
                   column_name, data_type, udt_name, is_nullable
            from information_schema.columns
            where table_schema in ('silver', 'gold')
            order by table_schema, table_name, ordinal_position
            """,
        ),
        "constraints": fetch_all(
            cursor,
            """
            select tc.table_schema as schema_name, tc.table_name, tc.constraint_name,
                   tc.constraint_type, kcu.column_name, kcu.ordinal_position,
                   ccu.table_schema as foreign_schema, ccu.table_name as foreign_table,
                   ccu.column_name as foreign_column
            from information_schema.table_constraints tc
            left join information_schema.key_column_usage kcu
              on kcu.constraint_schema = tc.constraint_schema
             and kcu.constraint_name = tc.constraint_name
             and kcu.table_name = tc.table_name
            left join information_schema.constraint_column_usage ccu
              on ccu.constraint_schema = tc.constraint_schema
             and ccu.constraint_name = tc.constraint_name
            where tc.table_schema in ('silver', 'gold')
              and tc.constraint_type in ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
            order by tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position
            """,
        ),
        "views": fetch_all(
            cursor,
            """
            select schemaname as schema_name, viewname as view_name, definition
            from pg_views where schemaname in ('silver', 'gold')
            order by schemaname, viewname
            """,
        ),
    }
    print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
