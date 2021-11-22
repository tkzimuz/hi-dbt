

  create  table
    dbo.`hi_item_cross_reference__dbt_tmp`
  as (
    
with __dbt__CTE__hi_item_cross_reference_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
select
    json_value(_airbyte_data, 
    '$."Item No_"') as `Item No_`,
    json_value(_airbyte_data, 
    '$."timestamp"') as `timestamp`,
    json_value(_airbyte_data, 
    '$."Description"') as `Description`,
    json_value(_airbyte_data, 
    '$."Variant Code"') as `Variant Code`,
    json_value(_airbyte_data, 
    '$."Unit of Measure"') as `Unit of Measure`,
    json_value(_airbyte_data, 
    '$."Item Disc_ Group"') as `Item Disc_ Group`,
    json_value(_airbyte_data, 
    '$."Cross-Reference No_"') as `Cross-Reference No_`,
    json_value(_airbyte_data, 
    '$."Cross-Reference Type"') as `Cross-Reference Type`,
    json_value(_airbyte_data, 
    '$."Discontinue Bar Code"') as `Discontinue Bar Code`,
    json_value(_airbyte_data, 
    '$."Cross-Reference Type No_"') as `Cross-Reference Type No_`,
    json_value(_airbyte_data, 
    '$."Extended Text Variant Code"') as `Extended Text Variant Code`,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    
    CURRENT_TIMESTAMP
 as _airbyte_normalized_at
from dbo._airbyte_raw_hi_item_cross_reference as table_alias
-- hi_item_cross_reference
where 1 = 1

),  __dbt__CTE__hi_item_cross_reference_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    cast(`Item No_` as char) as `Item No_`,
    cast(`timestamp` as char) as `timestamp`,
    cast(`Description` as char) as `Description Test`,
    cast(`Variant Code` as char) as `Variant Code`,
    cast(`Unit of Measure` as char) as `Unit of Measure`,
    cast(`Item Disc_ Group` as char) as `Item Disc_ Group`,
    cast(`Cross-Reference No_` as char) as `Cross-Reference No_`,
    cast(`Cross-Reference Type` as 
    float
) as `Cross-Reference Type`,
    cast(`Discontinue Bar Code` as 
    float
) as `Discontinue Bar Code`,
    cast(`Cross-Reference Type No_` as char) as `Cross-Reference Type No_`,
    cast(`Extended Text Variant Code` as char) as `Extended Text Variant Code`,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    
    CURRENT_TIMESTAMP
 as _airbyte_normalized_at
from __dbt__CTE__hi_item_cross_reference_ab1
-- hi_item_cross_reference
where 1 = 1

),  __dbt__CTE__hi_item_cross_reference_ab3 as (

-- SQL model to build a hash column based on the values of this record
select
    md5(cast(concat(coalesce(cast(`Item No_` as char), ''), '-', coalesce(cast(`timestamp` as char), ''), '-', coalesce(cast(`Description` as char), ''), '-', coalesce(cast(`Variant Code` as char), ''), '-', coalesce(cast(`Unit of Measure` as char), ''), '-', coalesce(cast(`Item Disc_ Group` as char), ''), '-', coalesce(cast(`Cross-Reference No_` as char), ''), '-', coalesce(cast(`Cross-Reference Type` as char), ''), '-', coalesce(cast(`Discontinue Bar Code` as char), ''), '-', coalesce(cast(`Cross-Reference Type No_` as char), ''), '-', coalesce(cast(`Extended Text Variant Code` as char), '')) as char)) as _airbyte_hi_item_cross_reference_hashid,
    tmp.*
from __dbt__CTE__hi_item_cross_reference_ab2 tmp
-- hi_item_cross_reference
where 1 = 1

)-- Final base SQL model
select
    `Item No_`,
    `timestamp`,
    `Description Test`,
    `Variant Code`,
    `Unit of Measure`,
    `Item Disc_ Group`,
    `Cross-Reference No_`,
    `Cross-Reference Type`,
    `Discontinue Bar Code`,
    `Cross-Reference Type No_`,
    `Extended Text Variant Code`,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    
    CURRENT_TIMESTAMP
 as _airbyte_normalized_at,
    _airbyte_hi_item_cross_reference_hashid
from __dbt__CTE__hi_item_cross_reference_ab3
-- hi_item_cross_reference from dbo._airbyte_raw_hi_item_cross_reference
where 1 = 1

  )
