{#- CFPB `Yes` / `No` text -> BOOL (null otherwise). -#}
{% macro cl_yes_no_bool(col) -%}
    case
        when {{ col }} = 'Yes' then true
        when {{ col }} = 'No' then false
        else null
    end
{%- endmacro %}

{#-
  CFPB company_response -> response_category in
  {explanation, monetary_relief, non_monetary_relief, in_progress, closed_other, untimely}.
  Legacy statuses (`Closed`, `Closed without relief`, `Closed with relief`) and any unknown non-null text
  become closed_other; null means the company has not answered yet (in_progress).
-#}
{% macro cl_response_category(col) -%}
    case {{ col }}
        when 'Closed with explanation' then 'explanation'
        when 'Closed with monetary relief' then 'monetary_relief'
        when 'Closed with non-monetary relief' then 'non_monetary_relief'
        when 'In progress' then 'in_progress'
        when 'Untimely response' then 'untimely'
        when 'Closed' then 'closed_other'
        when 'Closed without relief' then 'closed_other'
        when 'Closed with relief' then 'closed_other'
        else case when {{ col }} is null then 'in_progress' else 'closed_other' end
    end
{%- endmacro %}
