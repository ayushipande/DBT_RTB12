{% macro unload_model_to_snowflake(model_name, stage_name, file_name) %}
COPY INTO {{ stage_name }}/{{ file_name }}
FROM (
    SELECT * FROM {{ ref(model_name) }}
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
{% endmacro %}