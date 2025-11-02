# wpp-table-experiments
Experimental repo for WPP tables

## Outputs

- missing ids are present in output/missing_ids_labels.csv

- WPP table consisting of time scales is in output/wpp_table.csv

- got all ids and types from asctb table using all_asctb_ids_with_types.py

### Spatial Temporal Tables

Run in this sequence ->

- process_triples_withall_struc.py   (All structure for each entry with Fucntion@Process Triples)

- temporal_spatial_data_id_matched.py (Lowest sturcture/cell matched with ASCTB for each entry with Fucntion@Process Triples)

- Spatial_temporal_table.py (Final spatial temporal table)

### Challenges

Cannot find IDs of majority lower structures in asctb tables hence matched with the ones as lower as possible in hierarchy.