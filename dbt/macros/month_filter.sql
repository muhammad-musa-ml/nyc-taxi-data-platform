{#- A WHERE-clause fragment that restricts a model or test to the month in var('source_month').
    With the variable unset it is `true`, so the same SQL runs over every loaded month. -#}
{% macro month_filter(column='source_month') -%}
    {%- if var('source_month', none) is not none -%}
        {{ column }} = '{{ var("source_month") }}'::date
    {%- else -%}
        true
    {%- endif -%}
{%- endmacro %}
