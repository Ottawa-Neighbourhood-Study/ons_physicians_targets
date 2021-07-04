## note ethis is from the past study, haven't updated it yet. won't work yet
# added onsr::ottawa_db_pops_2016 so we don't need to load that from a file



#############################################
# POP WEIGHTING FOR ALL RESIDENTS
# Get population-weighted avg travel times and distances for all residents.
# So we use DB populations for weighting, then aggregate up to the neighbourhood
# level.
ons_pop_weight_dbs <- function(od_table, n_closest = 5, verbose = TRUE, also_do_ottawa = FALSE){
  # basic input validation
  if (!"DBUID" %in% colnames(od_table)) stop ("Input data does not have a column named `DBUID`. This function is designed to work with DB-level data.")
  if (!"distance" %in% colnames(od_table) | (!"time" %in% colnames(od_table))) stop ("Input data does not have a column named `distance` or `time`. This function is designed to work with output from valhalr::od_table().")
  if ((n_closest < 1) | (round(n_closest) != n_closest)) stop ("Number of closest facilities must be an integer greater than 0.")
  
  # get closest and shortest
  if (verbose) message (paste0("Calculating average distance to ", n_closest, " closest facilities.."))
  closest <- od_table %>%
    dplyr::group_by(DBUID) %>%
    dplyr::arrange(DBUID, distance) %>%
    dplyr::slice_head(n = n_closest) %>%
    dplyr::summarise(avg_dist = mean(distance, na.rm = TRUE)) %>%
    dplyr::distinct() %>%
    dplyr::mutate(DBUID = as.character(DBUID))
  
  if (verbose) message (paste0("Calculating average time to ", n_closest, " closest facilities.."))
  shortest <- od_table %>%
    dplyr::group_by(DBUID) %>%
    dplyr::arrange(DBUID, time) %>%
    dplyr::slice_head(n = n_closest) %>%
    dplyr::summarise(avg_time = mean(time, na.rm = TRUE)/60) %>%
    dplyr::distinct() %>%
    dplyr::mutate(DBUID = as.character(DBUID))
  
  # create table with each db', adnd's avg dist and avg time to 5 closest
  closest_shortest <- left_join(closest, shortest, by = "DBUID")
  
  # get single-link indicator (SLI) that maps each DB to one and only one nbhd
  db_sli <- onsr::get_db_to_ons()

  # get 2016 DB populations, which we've stored in the onsr package  
  db_pops <- onsr::ottawa_db_pops_2016 %>%
    dplyr::mutate(DBUID = as.character(DBUID))

  
  # get each DB census population, use the population to weight,
  # group by ONS_ID and summarise.
  if (verbose) message ("Creating population-weighted values at neighbourhood level...")
  ons_table <- db_pops %>%
    dplyr::left_join(closest_shortest, by = "DBUID") %>%
    dplyr::left_join(db_sli, by = "DBUID") %>%
    dplyr::mutate(weighted_dist = avg_dist * db_pop_2016,
                  weighted_time = avg_time * db_pop_2016) %>%
    dplyr::group_by(ONS_ID) %>%
    dplyr::mutate(ons_pop = sum(db_pop_2016, na.rm = TRUE)) %>%
    dplyr::arrange(ONS_ID) %>%
    dplyr::summarise(weighted_dist_ons = sum(weighted_dist, na.rm = TRUE)/ons_pop,
                     weighted_time_ons = sum(weighted_time, na.rm = TRUE)/ons_pop,
                     .groups = "drop") %>%
    dplyr::distinct() %>%
    dplyr::arrange(desc(weighted_time_ons)) %>%
    tidyr::drop_na()
  
  # we can also calculate a value for Ottawa overall
  # same algorithm, we just don't group by ONS_ID
  if (also_do_ottawa) {
    if (verbose) message ("Creating population-weighted values at Ottawa level...")
    ott_table <- db_pops %>%
      dplyr::left_join(closest_shortest, by = "DBUID") %>%
      dplyr::left_join(db_sli, by = "DBUID") %>%
      dplyr::mutate(weighted_dist = avg_dist * db_pop_2016,
                    weighted_time = avg_time * db_pop_2016) %>%
      dplyr::mutate(ons_pop = sum(db_pop_2016, na.rm = TRUE)) %>%
      dplyr::summarise(weighted_dist_ons = sum(weighted_dist, na.rm = TRUE)/ons_pop,
                       weighted_time_ons = sum(weighted_time, na.rm = TRUE)/ons_pop,
                       .groups = "drop") %>%
      dplyr::distinct() %>%
      dplyr::arrange(desc(weighted_time_ons)) %>%
      tidyr::drop_na() %>%
      mutate(ONS_ID = 0)
    
    ons_table <- bind_rows(ons_table, ott_table)

  }
  
  return (ons_table)
}
