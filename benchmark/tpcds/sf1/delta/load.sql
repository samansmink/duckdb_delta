SET VARIABLE delta_path = './data/generated/delta_rs_tpcds_sf1';

create view call_center as from delta_scan(getvariable('delta_path') || '/call_center/delta_lake');
create view catalog_page as from delta_scan(getvariable('delta_path') || '/catalog_page/delta_lake');
create view catalog_returns as from delta_scan(getvariable('delta_path') || '/catalog_returns/delta_lake');
create view catalog_sales as from delta_scan(getvariable('delta_path') || '/catalog_sales/delta_lake');
create view customer as from delta_scan(getvariable('delta_path') || '/customer/delta_lake');
create view customer_demographics as from delta_scan(getvariable('delta_path') || '/customer_demographics/delta_lake');
create view customer_address as from delta_scan(getvariable('delta_path') || '/customer_address/delta_lake');
create view date_dim as from delta_scan(getvariable('delta_path') || '/date_dim/delta_lake');
create view household_demographics as from delta_scan(getvariable('delta_path') || '/household_demographics/delta_lake');
create view inventory as from delta_scan(getvariable('delta_path') || '/inventory/delta_lake');
create view income_band as from delta_scan(getvariable('delta_path') || '/income_band/delta_lake');
create view item as from delta_scan(getvariable('delta_path') || '/item/delta_lake');
create view promotion as from delta_scan(getvariable('delta_path') || '/promotion/delta_lake');
create view reason as from delta_scan(getvariable('delta_path') || '/reason/delta_lake');
create view ship_mode as from delta_scan(getvariable('delta_path') || '/ship_mode/delta_lake');
create view store as from delta_scan(getvariable('delta_path') || '/store/delta_lake');
create view store_returns as from delta_scan(getvariable('delta_path') || '/store_returns/delta_lake');
create view store_sales as from delta_scan(getvariable('delta_path') || '/store_sales/delta_lake');
create view time_dim as from delta_scan(getvariable('delta_path') || '/time_dim/delta_lake');
create view warehouse as from delta_scan(getvariable('delta_path') || '/warehouse/delta_lake');
create view web_page as from delta_scan(getvariable('delta_path') || '/web_page/delta_lake');
create view web_returns as from delta_scan(getvariable('delta_path') || '/web_returns/delta_lake');
create view web_sales as from delta_scan(getvariable('delta_path') || '/web_sales/delta_lake');
create view web_site as from delta_scan(getvariable('delta_path') || '/web_site/delta_lake');