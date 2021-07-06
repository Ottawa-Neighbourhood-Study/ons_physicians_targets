##########################################################
# UPDATING OTTAWA NEIGHBOURHOOD STUDY (ONS) PHYSICIAN DATA
# Targets file for reproducible analysis and reports.
#
# PLEASE NOTE! This analysis is built in R using the package 'targets', which
# is designed for reproducible scientific research. It forces you to use a
# structured approach where you 1. define all of your inputs, 2. define all of
# your functions, and then 3. use your inputs and your functions to generate
# outputs, which can then be new inputs to new functions. It keeps track of
# which outputs are up to date, and will only re-run code when something upstream
# has changed.
#
# It's a bit tricky at first but it forces you to be disciplined and organized.
# No more 'analysis1aa-FINAL.R' generating 'output2.csv' that needs to be renamed
# before you run 'analysis2bb-2020.R'....
#
# Read more here: https://books.ropensci.org/targets/

# load the two packages we need to run targets and generate Rmd files
library(targets)
library(tarchetypes)

# tell targets which packages we need to load to do the analysis
tar_option_set(packages = c("tidyverse",
                            "ggspatial",
                            "onsr",
                            "valhallr",
                            "sf",
                            "tictoc",
                            "leaflet",
                            "readxl",
                            "ggplot2"))

# load all our custom functions stored in separate .R files
#source ("R/functions.R")
source("R/walking_analysis.R")
source("R/pop_weight_avg.R")


# define functions that point to our input files
path_to_all_physician_data <- function(){
  "data/docs_all_2021-06-30.csv"
}


# THIS IS THE MAIN TARGETS BODY. It's a list of tar_targets() where the first
# line is its internal name (basically a variable name), and the second line(s)
# is the function call that generates its value. tar_targets() that point to
# files also have the format = "file" flag for internal bookkeeping.

list(
  ##########################
  # LOAD INPUT FILES

  # ONS-related shapes
  tar_target(
    ottawa_shp,
    onsr::get_ons_shp() %>%
      sf::st_union() %>%
      sf::st_as_sf() %>%
      mutate(ONS_ID = 0,
             Name = "Ottawa",
             Name_FR = "") %>%
      rename(geometry = x) %>%
      sf::st_transform(crs = 32189)
  ),
  
  tar_target(
    ons_shp,
    onsr::get_ons_shp() %>%
      sf::st_transform(crs = 32189) %>%
      bind_rows(ottawa_shp)
  ),
  tar_target(
    ons_buffer,
    ons_shp %>%
      sf::st_union() %>%
      sf::st_buffer(50000) %>%
      sf::st_as_sf()
  ),
  # create a shapefile that contains each neighbourhood expanded by a 50m buffer
  tar_target(
    ons_buffer_50m,
    ons_shp %>%
      sf::st_buffer(50) %>%
      sf::st_as_sf()
  ),
  
  # pull ONS data from the server
  tar_target(
    ons_data,
    onsr::get_ons_data()
  ),
  
  # get population of each neighbourhood
  tar_target(
    nbhd_pop2016,
    ons_data %>%
      filter(polygon_attribute == "pop2016") %>%
      select(ONS_ID, 
             pop2016 = value) %>%
      mutate(ONS_ID = as.numeric(ONS_ID))
  ),
  
  # Physician data
  # Raw data
  tar_target(
    physician_data_raw,
    readr::read_csv(path_to_all_physician_data())
  ),
  # in a shapefile for geometric operations (CONSIDER COMBINING)
  tar_target(
    ottawa_docs,
    physician_data_raw %>%
      filter(family_physician) %>%
      #select(doc_name, lat, lng) %<%
      # drop_na(lat, lng) %>%
      #  filter(lng < 0 & lat > 40) %>%
      sf::st_as_sf(coords = c("lng", "lat"), crs = "WGS84", remove = FALSE) %>%
      sf::st_transform(crs = 32189) %>%
      rename(lon = lng)
  ),
  
  # Travel distance from Valhalla
  # note: this year's values are from the ISM study, calculated from DB centroids
  # to physicians using Valhalla and the valhallr R package
  # done external to this workflow since a) it's already done, b) it takes 
  # like an hour
  # walk time from each DB to each physician
  # looks complicated to have 3 targets (path to file, reading file, calculating)
  # but we need to 1. know if the file changes, 2. have the raw table for
  # calculating metric #5, and 
  tar_target(
    path_to_walk_table,
    "data/valhalla_matrix_pedestrian_all_target.csv",
    format = "file"
  ),
  # tar_target(
  #   walk_table,
  #   read_csv(path_to_walk_table)
  # ),
  # tar_target(
  #   ons_walk_table,
  #   ons_pop_weight_dbs(walk_table)
  # ),
  # tar_target(
  #   path_to_ons_auto_table,
  #   "data/ons_table_auto_rurality.csv",
  #   format = "file"
  # ),
  tar_target(
    path_to_auto_table,
    "data/valhalla_matrix_auto_all_target.csv",
    format = "file"
  ),

  # tar_target(
  #   ons_auto_table,
  #   read_csv(path_to_ons_auto_table)
  # ),
  


  
  # calculate some values!
  # 1. Physicians per Neighbourhood - Polygon attribute D_phy_man_count
  tar_target(
    fps_per_nhood,
    onsr::get_pts_neighbourhood(pts = ottawa_docs, pgon = ons_shp) %>%
      sf::st_set_geometry(NULL) %>%
      dplyr::group_by(ONS_ID) %>%
      dplyr::summarise(num_fps = n()) %>%
      onsr::add_back_nbhds(var = "num_fps") %>%
      arrange(ONS_ID) %>%
      drop_na() # exclude any docs outside of Ottawa
  ),
  
  ##2. # FPs / 1000 residents in the neighbourhood plus a 50m buffer
  tar_target(
    fps_per_nbhd_50m_buffer,
    
    {
      # get # of physicians in each neighbourhood plus a 50m buffer
      fps_per_nbhd_50m_buffer <- onsr::get_pts_neighbourhood(pts = ottawa_docs, pgon = ons_buffer_50m) %>%
        sf::st_set_geometry(NULL) %>%
        dplyr::group_by(ONS_ID, Name) %>%
        dplyr::summarise(num_fps = n()) %>%
        onsr::add_back_nbhds(var = "num_fps") %>%
        arrange(ONS_ID) %>%
        select(-Name)
      
      # create new data with comparison for inspection
      fps_with_buffer <- left_join(fps_per_nbhd_50m_buffer,
                                   nbhd_pop2016,
                                   by = "ONS_ID") %>%
        mutate(fps_per_1000_pop = (num_fps / pop2016) * 1000) %>%
        filter(!is.na(ONS_ID))
      
      fps_with_buffer %>%
        select(ONS_ID, fps_per_1000_pop)
      
    }
  ),
  
  ## Metric 3: Average driving time to 5 nearest FPs (measured from DBs and population-weighted up to the neighbourhood level)
  tar_target(
    # NOTE! don't run valhalla again; we have this info right now from the ISM study.
    avg_time_drive_5_fps,
    # read the big OD table, do population weighting, then set values to NA for
    # the two cemeteries and carleton.
    read_csv(path_to_auto_table) %>%
      ons_pop_weight_dbs(also_do_ottawa = TRUE) %>%
      select(ONS_ID, avg_time_drive_5_fps = weighted_time_ons) %>%
      mutate(avg_time_drive_5_fps= if_else(ONS_ID %in% c(5, 17, 71), NA_real_, avg_time_drive_5_fps))
     #ons_auto_table %>%
     # select(ONS_ID, Name, avg_time_drive_5_fps = time_all)
  ),
  
  ## Metric 4: Average walking distance to 5 nearest FPs (measured from 
  # dissemination blocks (DBs) and population-weighted up neighbourhood level)
  tar_target(
    # NOTE! don't run valhalla again; we have this info right now from the ISM study.
    avg_dist_walk_5_fps,
    # read the big OD table, do population weighting, then set values to NA for
    # the two cemeteries and carleton.
    read_csv(path_to_walk_table) %>%
      ons_pop_weight_dbs(also_do_ottawa = TRUE) %>%
      select(ONS_ID, avg_dist_walk_5_fps = weighted_dist_ons) %>%
      mutate(avg_dist_walk_5_fps= if_else(ONS_ID %in% c(5, 17, 71), NA_real_, avg_dist_walk_5_fps))
    # ons_walk_table %>%
    #   select(ONS_ID, Name, avg_dist_walk_5_pfs = dist_all)
  ),
  
  
  ## Metric 5: % of residents within a 15-minute walk of any FP
  # NOTE! This one requires more calculation and data sources.
  # We're also using data from the ISM study here: DB-to-doc OD table
  # And statscan data: 2016 DB populations
  # And ONS data: DB-to-neighbourhood single-link indicator
  tar_target(
    pct_15min_walk_fp,
    readr::read_csv(path_to_walk_table) %>%
      get_pct_within_walkdistance(minute_threshold=15) %>%
      mutate(D_phy_covPop= if_else(ONS_ID %in% c(5, 17, 71), NA_real_, D_phy_covPop)) %>%
      rename(pct_15min_walk_fp = D_phy_covPop)
  ),
  
  
  ### put it all together
  tar_target(
    new_data,
    fps_per_nhood %>%
      left_join(fps_per_nbhd_50m_buffer) %>%
      left_join(avg_time_drive_5_fps) %>%
      left_join(avg_dist_walk_5_fps) %>%
      left_join(pct_15min_walk_fp) %>%
      left_join(sf::st_set_geometry(onsr::ons_shp, NULL) %>% select(ONS_ID, Name)) %>%
      mutate(Name = if_else(ONS_ID == 0, "Ottawa", Name)) %>%
      select(ONS_ID, Name, everything()) %>%
      write_csv("outputs/new_data_human_readable.csv")
  ),
  
  
  ### rename and pivot to be in the right format for the ONS website
  tar_target(
    pivoted_data,
    new_data %>% 
      select(-Name) %>%
      pivot_longer(-ONS_ID, names_to = "polygon_attribute") %>% 
      pivot_wider(id_cols = "polygon_attribute", names_from = ONS_ID, values_from = "value") %>%
      write_csv("outputs/new_data_ons_format.csv")
  ),
  
  
  ## Ottawa docs KML file for the web site
  #group by lat & lon, combine all names for those
  # with same lat/lon, remove duplicate rows
  tar_target(
    ottawa_docs_kml,
    
    ottawa_docs %>%
      select(doc_name, lat, lon) %>%
      group_by(lat, lon) %>%
      mutate(count = n()) %>%
      mutate(physician_names = stringr::str_flatten(doc_name, collapse = "\n")) %>%
      select(lat, lon, physician_names) %>%
      sf::write_sf("outputs/physician_locations_for_google_maps.kml")
  ),
  
  NULL
)
