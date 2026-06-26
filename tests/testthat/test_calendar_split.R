context("STP-C multi-day cancellation calendar boundary")

# Helper to build a minimal schedule data frame accepted by splitDates /
# makeCalendar.inner.  Column names with spaces must be preserved.
make_cal <- function(uids, start_dates, end_dates, days, stps,
                     rowids = NULL) {
  n <- length(uids)
  if (is.null(rowids)) rowids <- rep(1L, n)
  data.frame(
    UID                  = uids,
    start_date           = as.Date(start_dates),
    end_date             = as.Date(end_dates),
    Days                 = days,
    STP                  = stps,
    rowID                = as.integer(rowids),
    Headcode             = rep("", n),
    "ATOC Code"          = rep("ZA", n),
    "Retail Train ID"    = rep("", n),
    "Train Status"       = rep("P", n),
    duration             = as.numeric(
      as.Date(end_dates) - as.Date(start_dates) + 1
    ),
    stringsAsFactors = FALSE,
    check.names      = FALSE
  )
}

# ── splitDates: multi-day STP=C (same day pattern as P) ───────────────────────
#
# Regression test for the "ghost service" bug:
#   A permanent Mon–Fri service with a 3-day STP=C cancellation (Wed–Fri
#   24–26 Jun 2026) was producing a post-cancellation P segment that started
#   on 2026-06-26 — the last day of the cancellation — instead of
#   2026-06-27 (the day after).
#
# Root cause: the date-trimming loop was modifying C rows as well as P rows.
# When the C row at position j-1 had its end_date decremented, the start-date
# check for the P row at position j would compare against the already-modified
# value and silently not fire, leaving the P segment one day too early.
#
# Fix: the loop now only processes P rows (guarded by `if (STP == "P")`), so
# the C row's end_date stays at its original value and the comparison at j
# correctly increments the P start_date.

test_that("P segment after multi-day STP=C starts the day AFTER the C ends", {
  cal <- make_cal(
    uids        = c("TEST01", "TEST01"),
    start_dates = c("2026-05-18", "2026-06-24"),
    end_dates   = c("2026-09-18", "2026-06-26"),
    days        = c("1111100",   "1111100"),   # same day pattern
    stps        = c("P",         "C")
  )

  result <- UK2GTFS:::splitDates(cal)

  expect_true(is.data.frame(result),
              info = "splitDates should return a data.frame")

  # The post-cancellation segment
  post <- result[result$start_date >= as.Date("2026-06-26"), ]
  expect_true(nrow(post) > 0,
              info = "should have at least one segment after the cancellation")

  earliest <- min(post$start_date)
  expect_gt(
    as.numeric(earliest),
    as.numeric(as.Date("2026-06-26")),
    label = paste0(
      "Post-cancellation P segment must start AFTER 2026-06-26 ",
      "(the STP=C DateTo); got ", earliest
    )
  )
})

# ── splitDates: multi-day STP=C with different day pattern ────────────────────
#
# Mirrors the actual L80073 scenario: permanent Mon–Fri service, cancelled
# only on Wed–Fri (Days="0011100") for 24–26 Jun.  The "split by day pattern"
# path in makeCalendar.inner groups P + C together because all C rows are
# always included regardless of day pattern.

test_that("P segment after multi-day STP=C (different day pattern) starts after C ends", {
  cal <- make_cal(
    uids        = c("L80073", "L80073"),
    start_dates = c("2026-05-18", "2026-06-24"),
    end_dates   = c("2026-09-18", "2026-06-26"),
    days        = c("1111100",   "0011100"),   # different day patterns
    stps        = c("P",         "C")
  )

  result <- UK2GTFS:::splitDates(cal)

  expect_true(is.data.frame(result))

  post <- result[result$start_date >= as.Date("2026-06-26"), ]
  expect_true(nrow(post) > 0)

  earliest <- min(post$start_date)
  expect_gt(
    as.numeric(earliest),
    as.numeric(as.Date("2026-06-26")),
    label = paste0(
      "Post-cancellation P segment must start AFTER the STP=C DateTo; got ",
      earliest
    )
  )
})

# ── makeCalendar.inner: single-day STP=C goes to calendar_dates ───────────────
#
# The simple path (exactly one P + one 1-day C, same STP group) should return
# the P in calendar and the C in calendar_dates as a GTFS exception, not
# attempt date-splitting.

test_that("single-day STP=C with one P produces a calendar_dates entry", {
  cal <- make_cal(
    uids        = c("TEST02", "TEST02"),
    start_dates = c("2026-01-01", "2026-06-26"),
    end_dates   = c("2026-12-31", "2026-06-26"),
    days        = c("1111100",   "1111100"),
    stps        = c("P",         "C")
  )

  result <- UK2GTFS:::makeCalendar.inner(cal)

  expect_true(is.list(result), info = "makeCalendar.inner should return a list")

  cal_out       <- result[[1]]
  cal_dates_out <- result[[2]]

  expect_equal(nrow(cal_out), 1L,
               info = "P schedule should produce a single calendar row")
  expect_true(is.data.frame(cal_dates_out),
              info = "single-day C should be returned as a calendar_dates data.frame")
  expect_equal(cal_dates_out$start_date[1], as.Date("2026-06-26"),
               info = "calendar_dates entry should be on the cancelled date")
})
