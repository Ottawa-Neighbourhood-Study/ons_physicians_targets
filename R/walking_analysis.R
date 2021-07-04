## Metric 5: % of residents within a 15-minute walk of any FP
# NOTE! This one requires more calculation and data sources.
# We're also using data from the ISM study here: DB-to-doc OD table
# And statscan data: 2016 DB populations
# And ONS data: DB-to-neighbourhood single-link indicator

get_pct_within_walkdistance <- function(walk_table, minute_threshold = 15) {
  #minute_threshold <- 15
  
  # group by DBUID, arrange in increasing order of time, get the top one (shortest),
  # then see if the shortest is under the threshold # of seconds
  pct_walking <- walk_table %>%
    drop_na() %>%
    group_by(DBUID) %>%
    arrange(time) %>%
    slice_head(n=1) %>%
    ungroup() %>%
    mutate(covered = if_else(time < minute_threshold * 60, TRUE, FALSE)) %>%
    #filter(DBUID == 35060560005)
    select(DBUID, covered) %>%
    
    left_join(onsr::ottawa_db_pops_2016, by = "DBUID") %>%
    mutate(DBUID = as.character(DBUID)) %>%
    left_join(onsr::db_to_ons_data, by = "DBUID") %>%
    mutate(covered_pop = covered * db_pop_2016) %>%
    #filter(ONS_ID == 48)
    group_by(ONS_ID) %>%
    summarise(total_pop = sum(db_pop_2016),
              covered_pop = sum(covered_pop),
              pct_covered = covered_pop/total_pop) %>%
    drop_na() %>%
    select(ONS_ID,
           D_phy_covPop = pct_covered)
  
  # do it also just for ottawa overall, like it's one big neighbourhood
  ott_pct_walking <- walk_table %>%
    drop_na() %>%
    group_by(DBUID) %>%
    arrange(time) %>%
    slice_head(n=1) %>%
    ungroup() %>%
    mutate(covered = if_else(time < minute_threshold * 60, 1, 0)) %>%
    select(DBUID, covered) %>%
    left_join(onsr::ottawa_db_pops_2016, by = "DBUID") %>%
    # mutate(DBUID = as.character(DBUID)) %>%
    # left_join(onsr::get_db_to_ons(), by = "DBUID") %>%
    mutate(ONS_ID = 0) %>%
    mutate(covered_pop = covered * db_pop_2016) %>%
    group_by(ONS_ID) %>%
    summarise(total_pop = sum(db_pop_2016),
              covered_pop = sum(covered_pop),
              pct_covered = covered_pop/total_pop) %>%
    drop_na() %>%
    select(ONS_ID,
           D_phy_covPop = pct_covered)
  
  # put the neighbourhood-level and ottawa-wide values together
  pct_walking <- bind_rows(ott_pct_walking, pct_walking)
  
  return (pct_walking)
}
