# Removing problems functions

#' Split a GTFS object based on trip_ids
#'
#' @param gtfs gtfs list
#' @param trip_ids a vector of trips ids
#' @export

gtfs_split_ids <- function(gtfs, trip_ids) {
  agency <- gtfs$agency
  stops <- gtfs$stops
  routes <- gtfs$routes
  trips <- gtfs$trips
  stop_times <- gtfs$stop_times
  calendar <- gtfs$calendar
  calendar_dates <- gtfs$calendar_dates


  trips_true <- trips[trips$trip_id %in% trip_ids, ]
  trips_false <- trips[!trips$trip_id %in% trip_ids, ]

  stop_times_true <- stop_times[stop_times$trip_id %in% trips_true$trip_id, ]
  stop_times_false <- stop_times[stop_times$trip_id %in% trips_false$trip_id, ]

  routes_true <- routes[routes$route_id %in% trips_true$route_id, ]
  routes_false <- routes[routes$route_id %in% trips_false$route_id, ]

  calendar_true <- calendar[calendar$service_id %in% trips_true$service_id, ]
  calendar_false <- calendar[calendar$service_id %in% trips_false$service_id, ]

  calendar_dates_true <- calendar_dates[calendar_dates$service_id %in% trips_true$service_id, ] # nolint
  calendar_dates_false <- calendar_dates[calendar_dates$service_id %in% trips_false$service_id, ] # nolint

  stops_true <- stops[stops$stop_id %in% stop_times_true$stop_id, ]
  stops_false <- stops[stops$stop_id %in% stop_times_false$stop_id, ]

  agency_true <- agency[agency$agency_id %in% routes_true$agency_id, ]
  agency_false <- agency[agency$agency_id %in% routes_false$agency_id, ]

  gtfs_true <- list(agency_true, stops_true, routes_true, trips_true, stop_times_true, calendar_true, calendar_dates_true) # nolint
  gtfs_false <- list(agency_false, stops_false, routes_false, trips_false, stop_times_false, calendar_false, calendar_dates_false) # nolint

  names(gtfs_true) <- c("agency", "stops", "routes", "trips", "stop_times", "calendar", "calendar_dates") # nolint
  names(gtfs_false) <- c("agency", "stops", "routes", "trips", "stop_times", "calendar", "calendar_dates") # nolint

  result <- list(gtfs_true, gtfs_false)
  names(result) <- c("true", "false")
  return(result)
}

#' Find fast trips
#' @description
#' Fast trips can idetify problems with the input data or converion process
#' This fucntion returns trip_ids for trips that exceed max speed.
#' @param gtfs list of gtfs tables
#' @param maxspeed the maximum allowed speed in metres per second default 30 m/s (about 70 mph) # nolint
#' @details
#' The fucntion looks a straightline distance between the first and middle stop
#' to allow for circular routes.
#' @export

gtfs_fast_trips <- function(gtfs, maxspeed = 30) {
  trips <- gtfs$stop_times
  #times$stop_sequence <- as.integer(times$stop_sequence) # nolint
  trips <- dplyr::left_join(trips, gtfs$stops, by = "stop_id")
  trips$distance <- geodist::geodist(as.matrix(trips[,c("stop_lon","stop_lat")]), sequential = TRUE, pad = TRUE) # nolint
  trips$distance[trips$stop_sequence == 1] <- NA
  trips$arrival_time <- as.POSIXct(trips$arrival_time, format="%H:%M:%S", origin = "1970-01-01") # nolint
  trips$time <- c(NA, difftime(trips$arrival_time[2:nrow(trips)], trips$arrival_time[1:(nrow(trips)-1)])) # nolint
  trips$speed <- trips$distance / trips$time
  trips$speed[trips$speed == Inf] <- NA

  times <- dplyr::group_by(trips, trip_id)
  times <- dplyr::summarise(times,
                            max_speed = max(speed, na.rm = TRUE)
  )
  times <- times[times$max_speed > maxspeed, ]
  return(times$trip_id)
}
#' gtfs_fast_trips <- function(gtfs, maxspeed = 30) {
#'   times <- gtfs$stop_times
#'   times$stop_sequence <- as.integer(times$stop_sequence)
#'   times <- dplyr::group_by(times, trip_id)
#'   times <- dplyr::summarise(times,
#'     nstops = dplyr::n(),
#'     time_start = arrival_time[stop_sequence == min(stop_sequence)],
#'     time_end = if (nstops == 2) {
#'       arrival_time[stop_sequence == max(stop_sequence)]
#'     } else {
#'       arrival_time[stop_sequence == stop_sequence[floor(stats::median(1:nstops))]]  # nolint
#'     },
#'     stop_start = stop_id[stop_sequence == min(stop_sequence)],
#'     stop_end = if (nstops == 2) {
#'       stop_id[stop_sequence == max(stop_sequence)]
#'     } else {
#'       stop_id[stop_sequence == stop_sequence[floor(stats::median(1:nstops))]]
#'     }
#'   )
#'
#'   stops <- gtfs$stops
#'   stops <- stops[, c("stop_id", "stop_lon", "stop_lat")]
#'   names(stops) <- c("stop_id", "from_lon", "from_lat")
#'   times <- dplyr::left_join(times, stops, by = c("stop_start" = "stop_id"))
#'   names(stops) <- c("stop_id", "to_lon", "to_lat")
#'   times <- dplyr::left_join(times, stops, by = c("stop_end" = "stop_id"))
#'   times$time_start <- lubridate::hms(times$time_start)
#'   times$time_end <- lubridate::hms(times$time_end)
#'   times$duration <- as.numeric(times$time_end - times$time_start)
#'   times$distance <- geodist::geodist(
#'     x = as.matrix(times[, c("from_lon", "from_lat")]),
#'     y = as.matrix(times[, c("to_lon", "to_lat")]),
#'     paired = TRUE
#'   )
#'   times$speed <- times$distance / times$duration
#'
#'   fast_trips <- times$trip_id[times$speed > maxspeed]
#'   return(fast_trips)
#' }


#' Clean simple errors from GTFS files
#'
#' @param gtfs gtfs list
#' @export
gtfs_clean <- function(gtfs) {
  # 0 Remove routes with no valid agency_id
  gtfs$routes <- gtfs$routes[gtfs$routes$agency_id %in% unique(gtfs$agency$agency_id), ]  # nolint

  # 1 Remove empty route_type
  #' gtfs$routes$route_type[is.na(gtfs$routes$route_type)] <- "-1"
  gtfs$routes <- gtfs$routes[!is.na(gtfs$routes$route_type), ]  # nolint

  # 2 Remove trips with no valid route_id
  gtfs$trips <- gtfs$trips[gtfs$trips$route_id %in% unique(gtfs$routes$route_id), ]  # nolint

  # 3 Remove stop times with no valid location or trip_id
  gtfs$stop_times <- gtfs$stop_times[gtfs$stop_times$stop_id %in% unique(gtfs$stops$stop_id), ]  # nolint
  gtfs$stop_times <- gtfs$stop_times[gtfs$stop_times$trip_id %in% unique(gtfs$trips$trip_id), ]  # nolint

  # 4 Remove stops that are never used
  gtfs$stops <- gtfs$stops[gtfs$stops$stop_id %in% unique(gtfs$stop_times$stop_id), ]  # nolint

  # 5 Remove calendar items that are never used
  gtfs$calendar <- gtfs$calendar[gtfs$calendar$service_id %in% unique(gtfs$trips$service_id), ]  # nolint
  gtfs$calendar_dates <- gtfs$calendar_dates[gtfs$calendar_dates$service_id %in% unique(gtfs$trips$service_id), ]  # nolint


  #' Replace "" agency_id with dummy name
  #' gtfs$agency$agency_id[is.na(gtfs$agency$agency_id)] <- "MISSINGAGENCY"
  #' gtfs$routes$agency_id[is.na(gtfs$routes$agency_id)] <- "MISSINGAGENCY"
  #' gtfs$agency$agency_id[is.na(gtfs$agency$agency_id)] <- "MISSINGAGENCY"
  #' gtfs$agency$agency_id[gtfs$agency$agency_id == ""] <- "MISSINGAGENCY"
  #' gtfs$routes$agency_id[gtfs$routes$agency_id == ""] <- "MISSINGAGENCY"
  #' gtfs$agency$agency_name[gtfs$agency$agency_name == ""] <- "MISSINGAGENCY"

  return(gtfs)
}
