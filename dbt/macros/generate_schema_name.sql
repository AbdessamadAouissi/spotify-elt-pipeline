{#
  Surcharge du comportement par défaut de dbt qui concatène
  "default_schema + custom_schema" (ex: spotify_silver_spotify_gold).

  Ici on veut que +schema: spotify_gold  →  dataset `spotify_gold` (tel quel).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
