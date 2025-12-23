################################################################################
# Lucas Abbenhuis — SIMPLE Rolling Time-Split Escalation Forecasting (ranger)
#
# Goal: predict escalation_next_week ("yes") for Armenia + Azerbaijan weekly data
#
# What this script does:
# 1) Build weekly dataset (ACLED + weather + media)
# 2) Create escalation label based on next week's events >= threshold
# 3) Rolling time split evaluation (forecast-like)
# 4) Compare persistence baseline vs Random Forest
# 5) Permutation importance + optional bootstrap uncertainty
# 6) Save tables + save plots to OUT_DIR

## Note: API Key still needs to be filled in (otherwise I couldnt upload to Github)
################################################################################


# ============================================================
# 1) MEDIA PIPELINE: Weekly sampling + API-based labeling + weekly features
# ============================================================

# 1.1 Libraries for media labeling and aggregation
library(httr)
library(jsonlite)
library(stringr)
library(readr)
library(progress)
library(dplyr)
library(lubridate)
library(ggplot2)

# Parallel labeling
library(future)
library(future.apply)

# 1.2 Working directory (media step)
setwd("C:/Users/lucas/Desktop/Data_science_applications")

# 1.3 Settings
input_file  <- "mc-onlinenews-mediacloud-20251221125256-content.csv"
output_sample_file <- "mc_weekly_sample_labeled.csv"
output_weekly_file <- "mc_weekly_features.csv"

api_key <- ""
model <- "gpt-4o-mini"

K_PER_WEEK <- 5
START_DATE <- as.Date("2017-01-01")
END_DATE   <- Sys.Date()

N_WORKERS <- 6
REQS_PER_MIN <- 120
MAX_RETRIES <- 6
SAVE_EVERY <- 50

# 1.4 Small helpers
`%||%` <- function(a, b) if (!is.null(a)) a else b
min_delay_sec <- 60 / REQS_PER_MIN

# 1.5 Prompt builder (metadata only)
build_prompt <- function(title, media_name, publish_date, language, url) {
  title <- str_squish(ifelse(is.na(title), "", title))
  media_name <- str_squish(ifelse(is.na(media_name), "", media_name))
  publish_date <- str_squish(ifelse(is.na(publish_date), "", publish_date))
  language <- str_squish(ifelse(is.na(language), "", language))
  url <- str_squish(ifelse(is.na(url), "", url))
  
  paste0(
    "Return ONLY valid JSON with keys: sentiment_label, sentiment_score, conflict_related, confidence.\n",
    "Rules:\n",
    "- sentiment_label: positive|neutral|negative\n",
    "- sentiment_score: number in [-1,1]\n",
    "- conflict_related: yes|no|uncertain\n",
    "- confidence: number in [0,1]\n\n",
    "Label news items about armed conflict/border clashes related to Nagorno-Karabakh.\n\n",
    "TITLE: ", title, "\n",
    "SOURCE: ", media_name, "\n",
    "DATE: ", publish_date, "\n",
    "LANGUAGE: ", language, "\n",
    "URL: ", url, "\n"
  )
}

# 1.6 OpenAI call with retries + backoff
send_to_gpt <- function(user_text, api_key, model = "gpt-4o-mini", max_retries = 6) {
  url <- "https://api.openai.com/v1/chat/completions"
  
  for (attempt in 0:max_retries) {
    Sys.sleep(min_delay_sec + runif(1, 0, min_delay_sec))
    
    resp <- tryCatch({
      POST(
        url = url,
        add_headers(Authorization = paste("Bearer", api_key)),
        content_type_json(),
        encode = "json",
        body = list(
          model = model,
          temperature = 0,
          messages = list(
            list(role = "system", content = "Output JSON only. No extra text."),
            list(role = "user", content = user_text)
          )
        ),
        timeout(120)
      )
    }, error = function(e) NULL)
    
    if (is.null(resp)) {
      wait <- min(60, 2^attempt + runif(1))
      Sys.sleep(wait)
      next
    }
    
    status <- status_code(resp)
    
    if (status >= 200 && status < 300) {
      parsed <- content(resp, as = "parsed", encoding = "UTF-8")
      if (is.list(parsed) && !is.null(parsed$choices) && length(parsed$choices) > 0) {
        return(parsed$choices[[1]]$message$content %||% NA_character_)
      } else {
        return(NA_character_)
      }
    }
    
    if (status %in% c(429, 500, 502, 503, 504)) {
      wait <- min(120, (2^attempt) + runif(1, 0, 2))
      Sys.sleep(wait)
      next
    } else {
      txt <- content(resp, as = "text", encoding = "UTF-8")
      message("HTTP error: ", status, " body: ", txt)
      return(NA_character_)
    }
  }
  
  NA_character_
}

# 1.7 Parse model output JSON safely
parse_model_json <- function(x) {
  if (is.null(x) || is.na(x) || !nzchar(x)) {
    return(list(sent_label = NA, sent_score = NA, conflict_related = NA, confidence = NA))
  }
  
  m <- str_extract(x, "\\{.*\\}")
  if (is.na(m)) m <- x
  
  obj <- tryCatch(fromJSON(m), error = function(e) NULL)
  if (is.null(obj)) {
    return(list(sent_label = NA, sent_score = NA, conflict_related = NA, confidence = NA))
  }
  
  sent_label <- tolower(obj$sentiment_label %||% NA_character_)
  sent_score <- suppressWarnings(as.numeric(obj$sentiment_score %||% NA_real_))
  conflict_related <- tolower(obj$conflict_related %||% NA_character_)
  confidence <- suppressWarnings(as.numeric(obj$confidence %||% NA_real_))
  
  if (!(sent_label %in% c("positive","neutral","negative"))) sent_label <- NA_character_
  if (!is.finite(sent_score) || sent_score < -1 || sent_score > 1) sent_score <- NA_real_
  if (!(conflict_related %in% c("yes","no","uncertain"))) conflict_related <- NA_character_
  if (!is.finite(confidence) || confidence < 0 || confidence > 1) confidence <- NA_real_
  
  list(sent_label = sent_label, sent_score = sent_score,
       conflict_related = conflict_related, confidence = confidence)
}

# 1.8 Load MediaCloud export + weekly sampling
mc <- read_csv(input_file, show_col_types = FALSE)

required_cols <- c("id","indexed_date","language","media_name","media_url","publish_date","title","url")
missing <- setdiff(required_cols, colnames(mc))
if (length(missing) > 0) stop(paste("Missing columns:", paste(missing, collapse = ", ")))

mc <- mc %>%
  mutate(
    publish_date = as.character(publish_date),
    day = as.Date(substr(publish_date, 1, 10))
  ) %>%
  filter(!is.na(day), day >= START_DATE, day <= END_DATE) %>%
  mutate(week = floor_date(day, unit = "week", week_start = 1))

set.seed(123)

weekly_sample <- mc %>%
  group_by(week) %>%
  mutate(rand = runif(n())) %>%
  arrange(rand, .by_group = TRUE) %>%
  slice_head(n = K_PER_WEEK) %>%
  ungroup() %>%
  select(-rand)

cat("Total rows in original file:", nrow(mc), "\n")
cat("Weeks covered:", n_distinct(weekly_sample$week), "\n")
cat("Sample size (rows to label):", nrow(weekly_sample), "\n")

# 1.9 Resume support (so reruns don’t lose progress)
if (file.exists(output_sample_file)) {
  prev <- read_csv(output_sample_file, show_col_types = FALSE)
  if (all(c("id","sent_label","sent_score","conflict_related","confidence") %in% colnames(prev))) {
    weekly_sample <- weekly_sample %>%
      left_join(prev %>% select(id, sent_label, sent_score, conflict_related, confidence), by = "id")
    cat("Resuming: loaded existing labeled sample file.\n")
  }
}

if (!("sent_label" %in% names(weekly_sample))) weekly_sample$sent_label <- NA_character_
if (!("sent_score" %in% names(weekly_sample))) weekly_sample$sent_score <- NA_real_
if (!("conflict_related" %in% names(weekly_sample))) weekly_sample$conflict_related <- NA_character_
if (!("confidence" %in% names(weekly_sample))) weekly_sample$confidence <- NA_real_

to_do <- which(is.na(weekly_sample$sent_label) |
                 is.na(weekly_sample$sent_score) |
                 is.na(weekly_sample$conflict_related) |
                 is.na(weekly_sample$confidence))

cat("Rows remaining to label:", length(to_do), "\n")

# 1.10 Parallel labeling (only runs if needed)
if (length(to_do) == 0) {
  cat("Nothing to label. Skipping API.\n")
} else {
  
  plan(multisession, workers = N_WORKERS)
  
  pb <- progress_bar$new(
    format = "  Labeling [:bar] :current/:total (:percent) eta: :eta",
    total = length(to_do),
    clear = FALSE,
    width = 60
  )
  
  chunk_ids <- split(to_do, ceiling(seq_along(to_do) / SAVE_EVERY))
  
  for (chunk in chunk_ids) {
    
    chunk_results <- future_lapply(chunk, function(idx) {
      prompt <- build_prompt(
        title = weekly_sample$title[idx],
        media_name = weekly_sample$media_name[idx],
        publish_date = weekly_sample$publish_date[idx],
        language = weekly_sample$language[idx],
        url = weekly_sample$url[idx]
      )
      
      resp <- send_to_gpt(prompt, api_key = api_key, model = model, max_retries = MAX_RETRIES)
      parsed <- parse_model_json(resp)
      
      list(
        idx = idx,
        sent_label = parsed$sent_label,
        sent_score = parsed$sent_score,
        conflict_related = parsed$conflict_related,
        confidence = parsed$confidence
      )
    })
    
    for (r in chunk_results) {
      weekly_sample$sent_label[r$idx] <- r$sent_label
      weekly_sample$sent_score[r$idx] <- r$sent_score
      weekly_sample$conflict_related[r$idx] <- r$conflict_related
      weekly_sample$confidence[r$idx] <- r$confidence
      pb$tick()
    }
    
    write_csv(weekly_sample, output_sample_file)
    cat("\nCheckpoint saved to:", output_sample_file, "\n")
  }
  
  write_csv(weekly_sample, output_sample_file)
  cat("\nSaved labeled sample to:", output_sample_file, "\n")
}

# 1.11 Aggregate labeled articles to weekly media features
weekly_features <- weekly_sample %>%
  group_by(week) %>%
  summarise(
    n_sample = n(),
    mean_sent = mean(sent_score, na.rm = TRUE),
    sd_sent   = sd(sent_score, na.rm = TRUE),
    share_neg = mean(sent_label == "negative", na.rm = TRUE),
    share_pos = mean(sent_label == "positive", na.rm = TRUE),
    share_neu = mean(sent_label == "neutral",  na.rm = TRUE),
    share_conflict_yes = mean(conflict_related == "yes", na.rm = TRUE),
    share_conflict_uncertain = mean(conflict_related == "uncertain", na.rm = TRUE),
    n_conflict_yes = sum(conflict_related == "yes", na.rm = TRUE),
    mean_confidence = mean(confidence, na.rm = TRUE),
    wmean_sent = {
      w <- confidence
      x <- sent_score
      ok <- is.finite(w) & is.finite(x) & w > 0
      if (sum(ok) == 0) NA_real_ else sum(w[ok] * x[ok]) / sum(w[ok])
    },
    .groups = "drop"
  )

write_csv(weekly_features, output_weekly_file)
cat("Saved weekly features to:", output_weekly_file, "\n")

# 1.12 Optional sanity plot for media sentiment
p_media <- ggplot(weekly_features, aes(x = week, y = mean_sent)) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed") +
  labs(
    title = paste0("Weekly mean sentiment (sampled ", K_PER_WEEK, " per week)"),
    x = "Week",
    y = "Mean sentiment score (-1 to +1)"
  ) +
  theme_minimal()

ggsave(filename = "weekly_mean_sentiment.png", plot = p_media, width = 10, height = 4)



# ============================================================
# 2) FORECASTING PIPELINE: Merge datasets + rolling evaluation + outputs
# ============================================================

# 2.1 Libraries for merging and modeling
library(readxl)
library(readr)
library(dplyr)
library(lubridate)
library(janitor)
library(ggplot2)
library(tibble)
library(ranger)
library(caret)

set.seed(7)

# 2.2 Paths for the forecasting step
setwd("C:/Users/lucas/Desktop/Data_science_applications/LU_BAP_CIR/datasets")
acled_path   <- "Europe-Central-Asia_aggregated_data_up_to-2025-11-29.xlsx"
weather_path <- "4191034(1).csv"
media_path   <- "mc_weekly_features.csv"

OUT_DIR <- "outputs_simple_rolling_runner"
dir.create(OUT_DIR, showWarnings = FALSE)

# 2.3 Settings
WEEK_START <- 1

USE_VIOLENT_ONLY <- TRUE
violent_types <- c("Battles", "Explosions/Remote violence", "Violence against civilians")

THR_GRID <- c(3, 5)

MIN_TRAIN_WEEKS <- 200
TEST_WEEKS      <- 26
STEP_WEEKS      <- 26

NTREE <- 2000
MTRY  <- NULL
MIN_NODE <- 5

USE_BOOTSTRAP_TEST <- TRUE
BOOT_REPS_TEST <- 500

# 2.4 Load datasets
acled   <- read_excel(acled_path) %>% clean_names()
weather <- read_csv(weather_path, show_col_types = FALSE) %>% clean_names()
media   <- read_csv(media_path, show_col_types = FALSE) %>% clean_names()

# 2.5 Build weekly ACLED table
acled <- acled %>%
  mutate(
    week = as.Date(week),
    events = as.numeric(events),
    fatalities = as.numeric(fatalities),
    population_exposure = as.numeric(population_exposure)
  ) %>%
  filter(country %in% c("Armenia", "Azerbaijan"))

if (USE_VIOLENT_ONLY) {
  acled <- acled %>% filter(event_type %in% violent_types)
}

acled_weekly <- acled %>%
  group_by(week) %>%
  summarise(
    events = sum(events, na.rm = TRUE),
    fatalities = sum(fatalities, na.rm = TRUE),
    exposure = sum(population_exposure, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(week)

all_weeks <- tibble(week = seq(min(acled_weekly$week), max(acled_weekly$week), by = "7 days"))
acled_weekly <- all_weeks %>%
  left_join(acled_weekly, by = "week") %>%
  mutate(across(c(events, fatalities, exposure), ~replace_na(.x, 0))) %>%
  arrange(week)

# 2.6 Build weekly weather table
weather_weekly <- weather %>%
  mutate(
    date = as.Date(date),
    week = floor_date(date, unit = "week", week_start = WEEK_START),
    prcp = as.numeric(prcp),
    snwd = as.numeric(snwd),
    tavg = as.numeric(tavg),
    tmax = as.numeric(tmax),
    tmin = as.numeric(tmin)
  ) %>%
  summarise(
    .by = week,
    temp_mean       = mean(tavg, na.rm = TRUE) / 10,
    temp_max_mean   = mean(tmax, na.rm = TRUE) / 10,
    temp_min_mean   = mean(tmin, na.rm = TRUE) / 10,
    precip_sum      = sum(prcp, na.rm = TRUE) / 10,
    snow_depth_mean = mean(snwd, na.rm = TRUE)
  ) %>%
  arrange(week)

# 2.7 Prepare media features as weekly table
media_weekly <- media %>%
  mutate(week = as.Date(week)) %>%
  transmute(
    week,
    gdelt_volume = as.numeric(n_sample),
    mean_sent = as.numeric(mean_sent),
    sd_sent = as.numeric(sd_sent),
    share_neg = as.numeric(share_neg),
    share_pos = as.numeric(share_pos),
    share_neu = as.numeric(share_neu),
    share_conflict_yes = as.numeric(share_conflict_yes),
    share_conflict_uncertain = as.numeric(share_conflict_uncertain),
    mean_confidence = as.numeric(mean_confidence)
  ) %>%
  arrange(week)

# 2.8 Merge + create predictors
df_base <- acled_weekly %>%
  left_join(weather_weekly, by = "week") %>%
  left_join(media_weekly, by = "week") %>%
  arrange(week) %>%
  mutate(across(where(is.numeric), ~replace_na(.x, 0))) %>%
  mutate(
    events_lag1     = lag(events, 1),
    events_lag2     = lag(events, 2),
    events_delta1   = abs(events - lag(events, 1)),
    fatalities_lag1 = lag(fatalities, 1),
    month           = month(week),
    week_of_year    = isoweek(week)
  ) %>%
  mutate(across(c(events_lag1, events_lag2, events_delta1, fatalities_lag1), ~replace_na(.x, 0)))

cat("\nData range:", as.character(min(df_base$week)), "to", as.character(max(df_base$week)), "\n")
cat("Weeks:", nrow(df_base), "\n\n")

ggsave(
  filename = file.path(OUT_DIR, "weekly_events.png"),
  plot = ggplot(df_base, aes(week, events)) + geom_line() +
    labs(title = "Weekly events (Armenia + Azerbaijan)", x = "Week", y = "Events"),
  width = 10, height = 4
)

# 2.9 Helper functions for labels, rolling splits, metrics, bootstrap
make_labeled_df <- function(df, thr_esc) {
  df %>%
    mutate(
      events_next = lead(events, 1),
      escalation_next_week = ifelse(events_next >= thr_esc, "yes", "no"),
      escalation_next_week = factor(escalation_next_week, levels = c("yes","no"))
    ) %>%
    filter(!is.na(events_next)) %>%
    select(-events_next)
}

make_rolling_splits <- function(df, min_train, test_weeks, step_weeks) {
  df <- df %>% arrange(week)
  n <- nrow(df)
  splits <- list()
  k <- 1
  start_test_i <- min_train + 1
  
  while ((start_test_i + test_weeks - 1) <= n) {
    train_idx <- 1:(start_test_i - 1)
    test_idx  <- start_test_i:(start_test_i + test_weeks - 1)
    
    splits[[k]] <- list(
      fold = k,
      train = df[train_idx, , drop = FALSE],
      test  = df[test_idx,  , drop = FALSE]
    )
    
    k <- k + 1
    start_test_i <- start_test_i + step_weeks
  }
  splits
}

predict_persistence <- function(df, thr) {
  factor(ifelse(df$events >= thr, "yes", "no"), levels = c("yes","no"))
}

eval_metrics <- function(pred, truth) {
  pred  <- factor(pred,  levels = c("yes","no"))
  truth <- factor(truth, levels = c("yes","no"))
  cm <- confusionMatrix(pred, truth, positive = "yes")
  
  tibble(
    Accuracy    = unname(cm$overall["Accuracy"]),
    Kappa       = unname(cm$overall["Kappa"]),
    Precision   = unname(cm$byClass["Pos Pred Value"]),
    Sensitivity = unname(cm$byClass["Sensitivity"]),
    Specificity = unname(cm$byClass["Specificity"]),
    F1          = unname(cm$byClass["F1"]),
    BalAcc      = unname(cm$byClass["Balanced Accuracy"])
  )
}

bootstrap_test_metrics <- function(test_df, thr, rf_model, reps = 500) {
  n <- nrow(test_df)
  out <- vector("list", reps)
  
  for (b in seq_len(reps)) {
    idx <- sample.int(n, size = n, replace = TRUE)
    tb <- test_df[idx, , drop = FALSE]
    
    truth <- tb$escalation_next_week
    pred_pers <- predict_persistence(tb, thr = thr)
    
    prob_rf <- predict(rf_model, data = tb)$predictions[, "yes"]
    pred_rf <- factor(ifelse(prob_rf >= 0.5, "yes", "no"), levels = c("yes","no"))
    
    out[[b]] <- bind_rows(
      eval_metrics(pred_pers, truth) %>% mutate(Model = "Persistence"),
      eval_metrics(pred_rf, truth)   %>% mutate(Model = "RF")
    ) %>% mutate(boot = b)
  }
  
  bind_rows(out)
}

# 2.10 Run rolling evaluation
all_fold_metrics <- list()
all_importance   <- list()
all_boot         <- list()
k <- 1
bidx <- 1

for (thr in THR_GRID) {
  
  cat("\n====================================================\n")
  cat("THR_ESC =", thr, "\n")
  cat("====================================================\n")
  
  df_lab <- make_labeled_df(df_base, thr_esc = thr)
  splits <- make_rolling_splits(df_lab, MIN_TRAIN_WEEKS, TEST_WEEKS, STEP_WEEKS)
  cat("Rolling folds:", length(splits), "\n")
  
  for (s in splits) {
    fold_id <- s$fold
    train_df <- s$train
    test_df  <- s$test
    
    cat(" Fold", fold_id, "| Test:", as.character(min(test_df$week)), "to", as.character(max(test_df$week)), "\n")
    
    if (length(unique(as.character(train_df$escalation_next_week))) < 2) {
      cat("   -> SKIP (train has only one class)\n")
      next
    }
    if (length(unique(as.character(test_df$escalation_next_week))) < 2) {
      cat("   -> SKIP (test has only one class)\n")
      next
    }
    
    rf_fit <- ranger(
      formula = escalation_next_week ~ .,
      data = train_df %>% select(-week),
      num.trees = NTREE,
      mtry = MTRY,
      min.node.size = MIN_NODE,
      probability = TRUE,
      importance = "permutation",
      seed = 7
    )
    
    pred_pers <- predict_persistence(test_df, thr = thr)
    prob_rf <- predict(rf_fit, data = test_df %>% select(-week))$predictions[, "yes"]
    pred_rf <- factor(ifelse(prob_rf >= 0.5, "yes", "no"), levels = c("yes","no"))
    truth <- test_df$escalation_next_week
    
    if (!exists("all_preds")) all_preds <- tibble()
    all_preds <- bind_rows(
      all_preds,
      tibble(
        THR_ESC = thr,
        Fold = fold_id,
        week = test_df$week,
        truth = truth,
        pred_pers = pred_pers,
        pred_rf = pred_rf,
        prob_rf = prob_rf
      )
    )
    
    fold_res <- bind_rows(
      eval_metrics(pred_pers, truth) %>% mutate(Model = "Persistence"),
      eval_metrics(pred_rf, truth)   %>% mutate(Model = "RF")
    ) %>% mutate(
      THR_ESC = thr,
      Fold = fold_id,
      TrainStart = min(train_df$week),
      TrainEnd   = max(train_df$week),
      TestStart  = min(test_df$week),
      TestEnd    = max(test_df$week),
      TrainN = nrow(train_df),
      TestN  = nrow(test_df),
      TestYes = sum(truth == "yes"),
      TestNo  = sum(truth == "no"),
      NTREE = NTREE,
      CV = "none (rolling eval only)"
    )
    
    all_fold_metrics[[k]] <- fold_res
    k <- k + 1
    
    imp <- enframe(rf_fit$variable.importance, name = "feature", value = "importance") %>%
      arrange(desc(importance)) %>%
      mutate(THR_ESC = thr, Fold = fold_id)
    
    all_importance[[length(all_importance) + 1]] <- imp
    
    if (USE_BOOTSTRAP_TEST) {
      boot_tbl <- bootstrap_test_metrics(
        test_df = test_df %>% select(-week),
        thr = thr,
        rf_model = rf_fit,
        reps = BOOT_REPS_TEST
      ) %>%
        mutate(THR_ESC = thr, Fold = fold_id)
      
      all_boot[[bidx]] <- boot_tbl
      bidx <- bidx + 1
    }
    
    saveRDS(rf_fit, file.path(OUT_DIR, paste0("rf_thr", thr, "_fold", fold_id, ".rds")))
  }
}

metrics_tbl <- bind_rows(all_fold_metrics)
imp_tbl     <- bind_rows(all_importance)
boot_tbl    <- if (length(all_boot) > 0) bind_rows(all_boot) else tibble()

write_csv(metrics_tbl, file.path(OUT_DIR, "rolling_metrics_folds.csv"))
write_csv(imp_tbl, file.path(OUT_DIR, "permutation_importance_folds.csv"))
if (nrow(boot_tbl) > 0) write_csv(boot_tbl, file.path(OUT_DIR, "bootstrap_test_metrics.csv"))

summary_tbl <- metrics_tbl %>%
  group_by(THR_ESC, Model) %>%
  summarise(
    Folds = n(),
    Acc_mean = mean(Accuracy, na.rm = TRUE),
    BalAcc_mean = mean(BalAcc, na.rm = TRUE),
    Sens_mean = mean(Sensitivity, na.rm = TRUE),
    Prec_mean = mean(Precision, na.rm = TRUE),
    F1_mean   = mean(F1, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(THR_ESC, desc(Sens_mean))

write_csv(summary_tbl, file.path(OUT_DIR, "rolling_summary_means.csv"))
cat("\n\n==================== SUMMARY (mean over folds) ====================\n")
print(summary_tbl)

# 2.11 Main plots (saved to OUT_DIR)
p1 <- ggplot(metrics_tbl, aes(x = factor(Fold), y = Sensitivity, color = Model, group = Model)) +
  geom_line() + geom_point() +
  facet_wrap(~ THR_ESC, scales = "free_y") +
  labs(title = "Rolling folds: Sensitivity (Recall YES)", x = "Fold", y = "Sensitivity")
ggsave(file.path(OUT_DIR, "sensitivity_by_fold.png"), p1, width = 9, height = 4)

imp_mean <- imp_tbl %>%
  group_by(THR_ESC, feature) %>%
  summarise(importance_mean = mean(importance, na.rm = TRUE), .groups = "drop") %>%
  arrange(THR_ESC, desc(importance_mean))
write_csv(imp_mean, file.path(OUT_DIR, "permutation_importance_mean.csv"))

# Extra plots you already added (kept as-is)
stopifnot(exists("metrics_tbl"), exists("imp_tbl"), exists("imp_mean"))
dir.create(OUT_DIR, showWarnings = FALSE)

top_k <- 15
imp_plot_df <- imp_mean %>%
  group_by(THR_ESC) %>%
  slice_max(order_by = importance_mean, n = top_k, with_ties = FALSE) %>%
  ungroup()

p_imp <- ggplot(imp_plot_df, aes(x = reorder(feature, importance_mean), y = importance_mean)) +
  geom_col() +
  coord_flip() +
  facet_wrap(~ THR_ESC, scales = "free_y") +
  labs(
    title = paste0("Permutation importance (mean over folds), Top ", top_k),
    x = "Feature",
    y = "Mean permutation importance"
  )
ggsave(filename = file.path(OUT_DIR, "importance_mean_top15.png"),
       plot = p_imp, width = 9, height = 6)

p_acc <- metrics_tbl %>%
  select(THR_ESC, Fold, Model, Accuracy, BalAcc) %>%
  tidyr::pivot_longer(cols = c(Accuracy, BalAcc), names_to = "Metric", values_to = "Value") %>%
  ggplot(aes(x = factor(Fold), y = Value, group = Model, color = Model)) +
  geom_line() + geom_point() +
  facet_grid(Metric ~ THR_ESC, scales = "free_y") +
  labs(
    title = "Rolling folds: Accuracy vs Balanced Accuracy",
    x = "Fold",
    y = "Metric value"
  )
ggsave(filename = file.path(OUT_DIR, "accuracy_vs_balacc_by_fold.png"),
       plot = p_acc, width = 10, height = 5)

if (exists("all_preds")) {
  
  make_cm_df <- function(df, pred_col) {
    df %>%
      transmute(truth = factor(truth, levels = c("yes","no")),
                pred  = factor(.data[[pred_col]], levels = c("yes","no"))) %>%
      count(truth, pred, name = "n") %>%
      tidyr::complete(truth, pred, fill = list(n = 0))
  }
  
  cm_rf <- all_preds %>%
    group_by(THR_ESC) %>%
    group_modify(~ make_cm_df(.x, "pred_rf")) %>%
    ungroup() %>%
    mutate(Model = "RF")
  
  cm_pers <- all_preds %>%
    group_by(THR_ESC) %>%
    group_modify(~ make_cm_df(.x, "pred_pers")) %>%
    ungroup() %>%
    mutate(Model = "Persistence")
  
  cm_plot_df <- bind_rows(cm_rf, cm_pers)
  
  p_cm <- ggplot(cm_plot_df, aes(x = pred, y = truth, fill = n)) +
    geom_tile() +
    geom_text(aes(label = n)) +
    facet_grid(Model ~ THR_ESC) +
    labs(
      title = "Confusion matrices aggregated over folds",
      x = "Predicted",
      y = "Actual"
    )
  
  ggsave(filename = file.path(OUT_DIR, "confusion_matrix_heatmap.png"),
         plot = p_cm, width = 9, height = 6)
}

if (exists("boot_tbl") && nrow(boot_tbl) > 0) {
  
  p_boot <- boot_tbl %>%
    select(THR_ESC, Fold, Model, Sensitivity, F1) %>%
    tidyr::pivot_longer(cols = c(Sensitivity, F1),
                        names_to = "Metric", values_to = "Value") %>%
    mutate(Value = as.numeric(Value)) %>%
    filter(is.finite(Value)) %>%
    ggplot(aes(x = Model, y = Value)) +
    geom_boxplot() +
    facet_grid(Metric ~ THR_ESC, scales = "free_y") +
    labs(
      title = "Bootstrap uncertainty in test metrics (by threshold)",
      x = "Model",
      y = "Metric value"
    )
  
  ggsave(filename = file.path(OUT_DIR, "bootstrap_uncertainty_boxplots.png"),
         plot = p_boot, width = 9, height = 6)
}

cat("\n\nDONE. Outputs written to:", file.path(getwd(), OUT_DIR), "\n")


# ============================================================
# 3) VARIABLE TABLE OUTPUT (CSV, no Word file)
# ============================================================

variable_table <- tibble::tribble(
  ~Variable, ~Description, ~Source, ~Aggregation,
  
  "events", "Total number of violent events in a given week", "ACLED", "Weekly sum",
  "fatalities", "Total number of reported fatalities in a given week", "ACLED", "Weekly sum",
  "events_lag1", "Number of violent events in the previous week", "ACLED", "Lagged weekly value",
  "events_lag2", "Number of violent events two weeks earlier", "ACLED", "Lagged weekly value",
  "events_delta1", "Absolute change in event count compared to the previous week", "ACLED", "Weekly difference",
  "fatalities_lag1", "Number of fatalities in the previous week", "ACLED", "Lagged weekly value",
  
  "month", "Calendar month indicator", "Constructed", "Monthly indicator",
  "week_of_year", "ISO week of the year", "Constructed", "Weekly indicator",
  
  "temp_mean", "Mean weekly temperature (°C)", "NOAA", "Weekly mean",
  "temp_max_mean", "Mean weekly maximum temperature (°C)", "NOAA", "Weekly mean",
  "temp_min_mean", "Mean weekly minimum temperature (°C)", "NOAA", "Weekly mean",
  "precip_sum", "Total weekly precipitation (mm)", "NOAA", "Weekly sum",
  "snow_depth_mean", "Mean weekly snow depth", "NOAA", "Weekly mean",
  
  "gdelt_volume", "Number of sampled news articles per week", "MediaCloud + sampling", "Fixed weekly sample (n = 5)",
  "mean_sent", "Mean sentiment score of sampled articles", "MediaCloud + API model", "Weekly mean",
  "sd_sent", "Standard deviation of sentiment scores", "MediaCloud + API model", "Weekly standard deviation",
  "share_neg", "Share of sampled articles classified as negative", "MediaCloud + API model", "Weekly proportion",
  "share_pos", "Share of sampled articles classified as positive", "MediaCloud + API model", "Weekly proportion",
  "share_neu", "Share of sampled articles classified as neutral", "MediaCloud + API model", "Weekly proportion",
  "share_conflict_yes", "Share of sampled articles classified as conflict-related", "MediaCloud + API model", "Weekly proportion",
  "share_conflict_uncertain", "Share of sampled articles classified as conflict-related (uncertain)", "MediaCloud + API model", "Weekly proportion",
  "mean_confidence", "Mean confidence score of classifications", "MediaCloud + API model", "Weekly mean",
  "wmean_sent", "Confidence-weighted mean sentiment score", "MediaCloud + API model", "Weekly weighted mean",
  
  "escalation_next_week", "Escalation indicator for week t+1 based on event threshold", "Constructed from ACLED", "Binary outcome"
)

write_csv(variable_table, file.path(OUT_DIR, "variable_table.csv"))
cat("\nSaved variable table to:", file.path(OUT_DIR, "variable_table.csv"), "\n")
