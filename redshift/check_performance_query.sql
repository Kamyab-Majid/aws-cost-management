select 
    REGEXP_SUBSTR(LOWER(before_change.query_text), 'user_event[_a-zA-Z0-9]*') as user_event_table, 
    REGEXP_COUNT(LOWER(before_change.query_text), 'user_event[_a-zA-Z0-9]*') as user_event_table_count,
    before_change.query_type, before_change.generic_query_hash, before_change.query_text, 
    after_change.max_elapsed_time/1000000 after_change_elapsed_time, before_change.max_elapsed_time/1000000 before_change_elapsed_time, 
    (after_change.max_elapsed_time-before_change.max_elapsed_time)/(1000000) elapsed_diff_sec
from
(select generic_query_hash, max(query_text) query_text,max(query_type) query_type, max(elapsed_time) max_elapsed_time
from ea_sys_query_history_p70112025
where trunc(start_time) < '07-07-2025'
and lower(query_text) like '%from%user_event%'
and username <> 'mkamyab@contractor.ea.com'
group by 1) before_change
JOIN 
(select generic_query_hash, max(query_text) query_text, max(query_type) query_type, max(elapsed_time) max_elapsed_time
from ea_sys_query_history_p70112025
where trunc(start_time) >= '07-07-2025'
and lower(query_text) like '%from%user_event%'
and   group by 1) after_change
ON before_change.generic_query_hash = after_change.generic_query_hash;
