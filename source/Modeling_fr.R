#------------------------------------------------------------------------------|
#------------ Step 0. Settings ------------------------------------------------
#------------------------------------------------------------------------------|
# load packages

.libPaths("/home/elap/R/lib")

pkg_list <- c("lubridate", "dplyr", "data.table", "ggplot2", "gridExtra", "zoo",
              "scales", "ranger", "gbm")
for(p in pkg_list){
    if (!is.element(p, installed.packages()[,1]))
        install.packages(p, dep = TRUE)
}
for(p in pkg_list) require(p, character.only = TRUE)

rm(list = ls())
gc(reset = T)
options(java.parameters = "-Xmx16g")


#------------------------------------------------------------------------------|
#------------ Step 1.Data Loading & Preprocessing -----------------------------
#------------------------------------------------------------------------------|

# Step 1-0. Criteria Time(timer per 15minutes)
NowTime <- Sys.time()
# NowTime <- as.POSIXct("2021-12-01 00:00:05", tz="Asia/Seoul")

NowYMDH <- lubridate::floor_date(NowTime, unit = "15 minutes")
CriYMDH <- lubridate::floor_date(NowTime - 15*60, unit = "15 minutes")

# Step 1-1. Load Dataset :: data with expected effect
# dat <- readRDS(file = paste(data_path, "DB04_업데이트데이터.RDS",sep="/"))
dat <- read.csv(file = "/home/elap/genieus_ai/DB_data/2021_2022_final_DB_fc.csv",header = TRUE, fileEncoding='UTF-8') # weather_ob_data

dat$DATE<-as.Date(dat$DATE, "%Y-%m-%d")
dat$ymdh <- as.POSIXct(dat$ymdh, tz = "Asia/Seoul")
print('load_data')
print((head(dat)))

# Step 1-2. Select Columns :: 모델링에 필요한 컬럼만 선택(추후 변수 추가에 따라 변경 가능)
dat <- dat[, c("DATE", "ymdh", "y", "WDAY", "HOUR", "timegrp", 
               "type_holiday", "holiday_yn", "longHoliday", "holi_ind1", "holi_ind2", 
               "사회적거리두기", "모임인원", "영업종료시간", 
               "평균기온", "평균강수량", 
               "effect_WDAYxHOUR", "effect_COVID1", "effect_COVID2", "effect_HOLI", 
               "effect_WEATHER1", "effect_WEATHER2", "effect_PAST", "effect_PAST2")]

dat <- dat %>% filter(DATE >= "2021-05-01")
idx <-nrow(dat)
ymdh.v <- dat$ymdh[idx]

# Step 1-3. Preprocessing : 데이터 처리
## Step 1-3-1. longHoliday NA값 변환 :: 연속 연휴에 해당하는 변수 NA를 0으로 변경
## Step 1-3-2. effect_COVID1         :: (y-effect_WDAYxHOUR) 기준으로 계산한 효과지만, 
#                                      코로나 효과이므로 절대값으로 변경
dat <- dat %>% mutate(longHoliday = ifelse(is.na(longHoliday), 0, longHoliday)
                      ,effect_COVID1 = abs(effect_COVID1))

# Step 1-3-3. 평균 기온과 강수량: 기존 전처리에서 미처리된 경우-->(앞시간 데이터로 변경)
#                          : 예보 데이터 사용시 발생할 경우는 없음.
dat$평균기온   <- zoo::na.locf(dat$평균기온, fromLast=F)
dat$평균강수량 <- zoo::na.locf(dat$평균강수량, fromLast=F)


# Step 1-4. PEAK TIME INDEX(피크 타임 인덱스 생성)
## Step 1-4-1. 영업 종료 시간을 기준으로 데이터 스프릿
closetime.dat.l <- split(dat, dat$영업종료시간)
## Step 1-4-2. 영업 종료 시간에 해당하는 시점과 15분뒤 시점에 해당하는 인덱스 생성
dat <- do.call('rbind', lapply(closetime.dat.l, function(tmp.ct.dat){
    # tmp.ct.dat <- closetime.dat.l[[1]]
    tmp.ct.dat$close.time <- as.character(as.numeric(gsub("[가-힣]", "", tmp.ct.dat$영업종료시간)))
    tmp.ct.dat$close.time <- ifelse(tmp.ct.dat$close.time == "24", "00", tmp.ct.dat$close.time)
    
    close.time <- unique(tmp.ct.dat$close.time)[1]
    t.values   <- paste0(close.time, c(":00:00", ":15:00"))
    
    tmp.ct.dat$PEAKTIME_IND <- ifelse((tmp.ct.dat$timegrp %in% t.values) , 1, 0)
    tmp.ct.dat <- tmp.ct.dat %>% mutate(CHCK = ifelse(HOUR == close.time, 1, 0), 
                                        close.time = NULL)
    return(tmp.ct.dat)
})) %>% arrange(ymdh)

## Step 1-4-3. 데이터 프레임 rownames(기본 인덱스) 재설정
#        : 데이터 스플릿 이름으로 인덱스 이름이 잡혀있어서 처리함
#        : Python에서 data.reset_index(drop=True) 와 같음

rownames(dat) <- 1:nrow(dat)

# Step 1-5. Factor 처리 변수
for(sel.v in c("WDAY", "HOUR", "timegrp", 
               "holiday_yn", "longHoliday", "holi_ind1", "holi_ind2",
               "사회적거리두기", "모임인원", "영업종료시간", 
               "PEAKTIME_IND")){
    if(sel.v == "WDAY"){ 
        dat[, sel.v] <- as.character(dat[, sel.v])
    }
    dat[, sel.v] <- as.factor(dat[, sel.v])
}

#------------------------------------------------------------------------------|
#------------ Step 2. Modeling Environment Settings ---------------------------
#------------------------------------------------------------------------------|

# Step 2-1. Formula Setting : 설명 변수 설정
var_list <- c("timegrp", "HOUR", "WDAY",
              "holiday_yn", "longHoliday", "holi_ind1", "holi_ind2", "PEAKTIME_IND",
              "사회적거리두기", "모임인원", "영업종료시간", 
              "평균기온", "평균강수량",
              "effect_WDAYxHOUR", "effect_COVID1", "effect_COVID2", "effect_HOLI",
              "effect_WEATHER1", "effect_WEATHER2", "effect_PAST", "effect_PAST2")

# Step 2-2. Data Setting
## Step 2-2-1. train/test 분리
train_df <- subset(dat, ymdh < ymdh.v)
test_df  <- subset(dat, ymdh >= ymdh.v)

## Step 2-2-2. 사용 변수 재정비
### Step 2-2-2-1. unique한 변수 제외 : 변수의 값이 1개인 경우는 학습할 수 없음
remove_var  <- names(train_df)[apply(train_df, 2, function(xx) length(unique(xx)) == 1)]
model_var   <- setdiff(var_list, c(remove_var))

### Step 2-2-2-2. Complete.cases Train : NA가 없는 완전한 데이터셋으로만 학습
c_train_df <- train_df[complete.cases(train_df[,c(model_var, 'y')]), c(model_var, 'y')]

### Step 2-2-2-3. c_train_df에서 unique한 변수 다시 제외 
remove_var3  <- names(c_train_df)[apply(c_train_df, 2, function(xx) length(unique(xx)) == 1)]
model_var2   <- setdiff(model_var, remove_var3)

### Step 2-2-2-4. train에는 없지만, test에 있는 값을 가진 컬럼은 학습이 불가능 하므로 제외..
#                 15분 단위 학습인 경우, 한 시점에서만 제외되며, 다음 시점에서는 제외되지 않음
remove_var4 <- c()
for(c in model_var2){#c <- model_var2[1]
    if(is.factor(c_train_df[[c]])){# c <- "사회적거리두기"
        tmp.uniq.v <- unique(c_train_df[[c]])
        # test_df에도 있는지 확인
        tmp.uniq.v.ts <- unique(test_df[[c]])
        chck.v        <- sum(!tmp.uniq.v.ts %in% tmp.uniq.v)
        if(chck.v != 0){
            remove_var4 <- c(remove_var4, c)
        }
    }
    
}
model_var3   <- setdiff(model_var2, remove_var4)
c_train_df   <- c_train_df[,c(model_var3, 'y')]

### Step 2-2-2-5. 최종 사용 변수
# 포뮬라 설정(종속 변수 ~ 설명변수1 + ... + 설명변수n)
formula.str <- paste0("y ~ ", paste0(paste(model_var3, collapse = "+")))
formula     <- as.formula(formula.str)


#------------------------------------------------------------------------------|
#------------ Step 3. Model 4: MLR + RF model ---------------------------------
#------------------------------------------------------------------------------|
print('lm_model')
## Step 3-3-1. MRL Model
lm_model          <- lm(formula, data = c_train_df)

## Step 3-3-2. Predict MRL
test_df$pred_lm   <- predict(lm_model, newdata = test_df)

## Step 3-3-3. Residual (for RF residual fitting)
### Step 3-3-3-1. MRL Residual
c_train_df$lm_res <- resid(lm_model)

print('Residual Fitting')
## Step 3-6-1. Residual Fitting
formula.res.str   <- paste0("lm_res ~ ", paste0(paste(model_var3, collapse = "+")))
formula.res       <- as.formula(formula.res.str)

## Step 3-6-2. MLR + RF Model
set.seed(2021)
rf.res.fit        <- ranger(formula.res, data=c_train_df)

## Step 3-6-3. Predict MLR + RF
rf.fit.test       <- predict(rf.res.fit, data = test_df)

## Step 3-6-4. Predict MLR + RF
### Step 3-6-4-1. RF Residual Fitting
test_df$pred_rf_res <- rf.fit.test$predictions
### Step 3-6-4-2. MLR Prediction + RF Residual Prediction
test_df$pred_lmrf   <- test_df$pred_lm + test_df$pred_rf_res


#------------------------------------------------------------------------------|
#------------ Step 4. Save Results --------------------------------------------
#------------------------------------------------------------------------------|
print('save result')
# Step 4-1. Results
save.cols    <- c("ymdh", "pred_lmrf","holiday_yn")
model_result <- test_df[, save.cols]

# Step 4-2. Save Results
f.nm <- "Result/modeling_result.csv"
write.csv(model_result, file="/home/elap/genieus_ai/Result/modeling_result.csv")
# write.csv(model_result, file="modeling_result.csv")

