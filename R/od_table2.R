

n_results <- length(results)
n_rows <- purrr::map_dbl(results, nrow)
total_rows <- sum(nrows)

# resultss <- tibble(distance = rep(NA_real_, total_rows))
# 
# resultss$distance[1:10000] = results[[1]]$distance

# results
# result
# 
# results[[1]]$distance %>% length()

output <- tibble(distance = rep(NA_real_, total_rows),
                   time = rep(NA_real_, total_rows),
                   from_id_column = rep(NA, total_rows),
                   to_id_column = rep(NA, total_rows)
                   )


#new <- bench::mark({
  for (i in 1:n_results) {
    message(i)
    # set our start and end indices
    index_start <- 1
    index_end <- n_rows[[1]]
    if (i != 1){
      index_start <- sum(n_rows[1:(i-1)]) + 1
      index_end <- sum(n_rows[1:i])
    }
    
    resultss$distance[index_start:index_end] <- results[[i]]$distance
    resultss$time[index_start:index_end] <- results[[i]]$time
    resultss$from_id_column[index_start:index_end] <- results[[i]][from_id_col] %>% unlist()
    resultss$to_id_column[index_start:index_end] <- results[[i]][to_id_col] %>% unlist()
  }
  
  resultss <- rename(resultss,
                     from_id_col = from_id_column,
                     to_id_col = to_id_column)
#})




resultss$distance[1:3] <- 5
resultss$distance[5:7] <- 50
resultss$distance


old <- bench::mark({  output <- results %>%
  tibble::enframe() %>%
  tidyr::unnest("value") %>%
  dplyr::select(from_id_col, to_id_col, "distance", "time")
})










od_table2 <- function(froms, from_id_col, tos, to_id_col, costing = "auto", batch_size = 100, minimum_reachability = 500, verbose = FALSE, hostname = "localhost", port = 8002){
  # note: got importFrom rlang trick here: https://stackoverflow.com/questions/58026637/no-visible-global-function-definition-for
  from_index <- to_index <- NULL
  
  # Basic input validation
  if ((!"lat" %in% colnames(froms)) | (!"lon" %in% colnames(tos))) stop("Input data `froms` and `tos` must both have columns named `lat` and `lon`. ")
  
  # temporarily rename the data columns so they're easier to work with
  # we'll put the names back right at the end
  froms <- rename(froms, .from_id_col = {{from_id_col}})
  tos   <- rename(tos, .to_id_col = {{to_id_col}})
  
  # get the human-readable names of the from- and to-data
  from_names <- froms %>%
    dplyr::select(.from_id_col) %>%
    tibble::rowid_to_column(var = "from_index")
  
  to_names <- tos %>%
    dplyr::select(.to_id_col) %>%
    tibble::rowid_to_column(var = "to_index")
  
  # if human-readable column names are identical, append "_from" and "_to" so they differ
  if (from_id_col == to_id_col) {
    #new_from <- paste0(from_id_col,"_from")
    #from_names <- dplyr::rename(from_names, !!(new_from) := from_id_col)
    from_id_col <- paste0(from_id_col,"_from")
    
    #new_to <- paste0(to_id_col, "_to")
    #to_names <- dplyr::rename(to_names, !!(new_to) := to_id_col)
    to_id_col <- paste0(to_id_col, "_to")
  }
  
  # set up our batching
  n_iters <- ceiling(nrow(froms) / batch_size)  #nrow(froms) %/% batch_size + 1 THIS MIGHT BE A BUG IN ORIGINAL.
  results <- list(rep(NA, n_iters))
  
  # do each batch
  for (i in 1:n_iters){
    if (verbose) message(paste0(i,"/",n_iters))
    start_index <- (i-1)*batch_size + 1
    end_index <- min( (i*batch_size), nrow(froms))
    
    froms_iter = froms[start_index:end_index, ] %>%
      tidyr::drop_na()
    
    od <- valhallr::sources_to_targets(froms = froms_iter,
                                       tos = tos,
                                       costing = costing,
                                       minimum_reachability = minimum_reachability,
                                       hostname = hostname,
                                       port = port)
    
    # POTENTIAL TODO: validate the API response from sources_to_targets()
    
    # make start_index match the original DB row number and doc row number
    od <- od %>%
      dplyr::mutate(from_index = from_index + start_index,
                    to_index = to_index + 1) %>%
      dplyr::left_join(from_names, by = "from_index") %>%
      dplyr::left_join(to_names, by = "to_index") %>%
      dplyr::select(-to_index, -from_index)
    
    # add results to our pre-built list
    results[[i]] <- od
  }
  
  # get results from a list of tibbles back into a single tibble
  # this has been rewritten speed and memory optimization
  # originally it was much shorter and used tidyr::unnest() but it was a major
  # bottleneck for large datasets (and would even fail)
  
  # get info about our results
  n_results <- length(results)
  n_rows <- purrr::map_dbl(results, nrow)
  total_rows <- sum(n_rows)
  
  # set up our final output tibble
  output <- tibble::tibble(.from_id_col = rep(NA, total_rows),
                           .to_id_col = rep(NA, total_rows),
                           distance = rep(NA_real_, total_rows),
                           time = rep(NA_real_, total_rows))
  
  # loop through each batch of results, move the results from the batch to the
  # final output tibble. a loop is much faster than using tidyr::unnest() here
  # because it modifies in place and isn't binding rows.
  for (i in 1:n_results) {
    
    # set our start and end indices
    index_start <- 1
    index_end <- n_rows[[1]]
    if (i != 1){
      index_start <- sum(n_rows[1:(i-1)]) + 1
      index_end <- sum(n_rows[1:i])
    }
    
    # overwrite the relevant bits of the vectors
    output$distance[index_start:index_end] <- results[[i]]$distance
    output$time[index_start:index_end] <- results[[i]]$time
    output$.from_id_col[index_start:index_end] <- results[[i]]$.from_id_col
    output$.to_id_col[index_start:index_end] <- results[[i]]$.to_id_col
  }
  
  # set the names back
  output <- dplyr::rename(output, 
                   {{from_id_col}} := .from_id_col,
                   {{to_id_col}} := .to_id_col)
  
  # voila
  return(output)
}




