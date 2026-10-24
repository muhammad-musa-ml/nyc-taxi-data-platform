{% test unique_combination(model, columns) %}
{#- Fails for every combination of `columns` that appears more than once. -#}
select {{ columns | join(', ') }}, count(*) as occurrences
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

{% test expression_is_true(model, expression, column_name=none) %}
{#- Fails for every row where `expression` is not true. -#}
select *
from {{ model }}
where not coalesce(({{ expression }}), false)
{% endtest %}
