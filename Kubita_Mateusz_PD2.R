
#ladowanie pakietow

#install.packages(c("sqldf", "dplyr", "data.table"))

library("sqldf")
library("dplyr")
library("data.table")
library("microbenchmark")
library("compare")
library("stringi")

options(stringsAsFactors=FALSE)



df_sql_1 <- function(Tags){
  #' funkcja z pakietu sqldf dla zadania 1 
  sql1 <- sqldf("
    SELECT Count, TagName
    FROM Tags
    WHERE Count > 1000
    ORDER BY Count DESC
")
}

df_base_1 <- function(Tags){
  #' funkcja z bazowego R dla zadania 1 
  r1 <- Tags[Tags$Count > 1000, c("Count","TagName") ]
  r1 <- r1[order(r1$Count, decreasing = TRUE),]
}

df_dplyr_1 <- function(Tags){
  #' funkcja z pakietu dplyr dla zadania 1 
  #korzystam z pipes (%>%) w pakiecie dplyr
  dplyr1 <- Tags %>% 
    filter(Count > 1000)  %>% 
    select(c("Count", "TagName")) %>% 
    arrange(desc(Count))
}


df_table_1 <- function(Tags){
  #' funkcja z pakietu data.table dla zadania 1 
  TagsDT <- as.data.table(Tags) #zmieniam klase na 'data.table', aby moc operowac funkcjami z tego pakietu
  dt1 <- TagsDT[Count>1000, TagName,Count]
  dt1 <- dt1[order(Count, decreasing = TRUE)]
}


df_sql_2 <- function(Posts, Users){
  #' funkcja z pakietu sqldf dla zadania 2
  sql2 <- sqldf("
    SELECT Location, COUNT(*) AS Count
    FROM (
        SELECT Posts.OwnerUserId, Users.Id, Users.Location
        FROM Users
        JOIN Posts ON Users.Id = Posts.OwnerUserId
    )
    WHERE Location NOT IN ('')
    GROUP BY Location
    ORDER BY Count DESC
    LIMIT 10
")
}

df_base_2 <- function(Posts, Users){
  #' funkcja z bazowego R dla zadania 2
  # I czesc
  r2 <- merge(x = Posts, y = Users,
              by.x = "OwnerUserId", by.y = "Id")
  r2 <- r2[, c("OwnerUserId", "Id", "Location")]
  r2 <- aggregate(x = r2$Location, by = r2["Location"], FUN = length)
  names(r2)[2] <- "Count"
  # II czesc
  r2 <- r2[r2$Location != "", ]
  r2 <- r2[order(r2$Count, decreasing = TRUE), ][1:10,]
}

df_dplyr_2 <- function(Posts, Users){
  #' funkcja z pakietu dplyr dla zadania 2
  # I czesc
  dplyr2 <- inner_join(x = Posts, y = Users,
                       by = c("OwnerUserId" = "Id"))
  dplyr2 <- dplyr2 %>% 
    select(c("OwnerUserId", "Id", "Location"))
  # korzystam z .groups = 'drop', aby nie tworzyc kolumny jako macierz tylko aby wszystko bylo na tym samym poziomie
  dplyr2 <- dplyr2 %>% 
    group_by(Location) %>% summarise(Count = length(Location), .groups = "drop")
  # II czesc
  dplyr2 <- dplyr2 %>% 
    filter(Location != "") %>% 
    arrange(desc(Count)) %>% 
    slice_head(n = 10)
}


df_table_2 <- function(Posts, Users){
  #' funkcja z pakietu data.table dla zadania 2
  PostsDT <- as.data.table(Posts)
  UsersDT <- as.data.table(Users)
  # I czesc
  dt2 <- merge(x = PostsDT, y = UsersDT,
               by.x = "OwnerUserId", by.y = "Id")
  dt2 <- dt2[, .(OwnerUserId,Id,Location)]
  dt2 <- dt2[,.(Count = .N), by = Location]
  # II czesc
  dt2 <- dt2[Location != "", ]
  dt2 <- dt2[order(Count, decreasing = TRUE)][1:10,]
}


df_sql_3 <- function(Badges){
  #' funkcja z pakietu sqldf dla zadania 3
  sql3 <- sqldf("
    SELECT Year, SUM(Number) AS TotalNumber
    FROM (
        SELECT
            Name,
            COUNT(*) AS Number,
            STRFTIME('%Y', Badges.Date) AS Year
        FROM Badges
        WHERE Class = 1
        GROUP BY Name, Year
        )
    GROUP BY Year
    ORDER BY TotalNumber")
}



df_base_3 <- function(Badges){
  #' funkcja z bazowego R dla zadania 3
  # I czesc
  tmp <- Badges
  tmp[, "Year"] <- stri_sub(  tmp[,c("Date")],1,4) #wyciagam rok z kolumny Date, czyli pierwsze 4 znaki 
  # 2 czesc
  r3 <- tmp[tmp$Class == 1, ]
  r3 <- aggregate(x = r3$Name, 
                  by = r3[, c("Name", "Year")], FUN = length)
  names(r3)[3] <- "Number"
  # 3 czesc
  r3 <- aggregate(x = r3["Number"],
                  by = r3["Year"],
                  FUN = sum)
  names(r3)[2] <- "TotalNumber"
  r3 <- r3[order(r3$TotalNumber), ]
}


df_dplyr_3 <- function(Badges){
  #' funkcja z pakietu dplyr dla zadania 3
  # 1 czesc
  #korzystam z funkcji pull, aby wyciagnac kolumne jako wektor a nie ramke danych
  dplyr3 <- Badges %>% 
    mutate(Year = stri_sub(Badges %>% pull(Date), 1,4))
  # 2 czesc
  dplyr3 <- dplyr3 %>% filter(Class == 1)
  dplyr3 <- dplyr3 %>% group_by(Name, Year) %>% summarise(Number = length(Name),
                                                          .groups = "drop")
  # 3 czesc
  dplyr3 <- dplyr3 %>% group_by(Year) %>% summarise(TotalNumber = sum(Number),
                                                    .groups = "drop")
  dplyr3 <- dplyr3 %>% arrange(TotalNumber)
}



df_table_3 <- function(Badges){
  #' funkcja z pakietu data.table dla zadania 3
  dt3 <- as.data.table(Badges)
  # 1 czesc
  dt3[, `:=` (Year = stri_sub( dt3[, Date],1,4))]
  # 2 czesc
  dt3 <- dt3[Class == 1]
  dt3 <- dt3[, .(Number = .N), by = .(Name, Year) ]
  # 3 czesc
  dt3 <- dt3[, .(TotalNumber = sum(Number)), by = Year]
  dt3 <- dt3[order(TotalNumber)]
}


df_sql_4 <- function(Users, Posts){
  #' funkcja z pakietu sqldf dla zadania 4
  sql4 <- sqldf("
    SELECT
        Users.AccountId,
        Users.DisplayName,
        Users.Location,
        AVG(PostAuth.AnswersCount) as AverageAnswersCount
    FROM
    (
        SELECT
            AnsCount.AnswersCount,
            Posts.Id,
            Posts.OwnerUserId
        FROM (
                SELECT Posts.ParentId, COUNT(*) AS AnswersCount
                FROM Posts
                WHERE Posts.PostTypeId = 2
                GROUP BY Posts.ParentId
                ) AS AnsCount
        JOIN Posts ON Posts.Id = AnsCount.ParentId
    ) AS PostAuth
    JOIN Users ON Users.AccountId=PostAuth.OwnerUserId
    GROUP BY OwnerUserId
    ORDER BY AverageAnswersCount DESC, AccountId ASC
    LIMIT 10
")
  
}

df_base_4 <- function(Users, Posts){
  #' funkcja z bazowego R dla zadania 4 
  # czesc I
  Posts_sub <- Posts[Posts$PostTypeId == 2,]
  AnsCount <- aggregate(x = Posts_sub$ParentId, by = Posts_sub["ParentId"], FUN = length)
  names(AnsCount)
  names(AnsCount)[2] <- "AnswersCount"
  # czesc II
  PostAuth <- merge(x = Posts, y = AnsCount,
                    by.x = "Id", by.y = "ParentId")
  PostAuth <- PostAuth[, c("AnswersCount", "Id", "OwnerUserId")]
  res <- merge(x = Users, y = PostAuth,
               by.x = "AccountId", by.y = "OwnerUserId")
  res <- aggregate(x = res["AnswersCount"], 
                   by = res[c("AccountId","DisplayName", "Location")], 
                   FUN = mean, na.rm = TRUE)
  names(res)[4] <- "AverageAnswersCount"
  # czesc III
  res <- res[order(res$AverageAnswersCount, res$AccountId, decreasing = c(TRUE,FALSE)),][1:10,]
}

df_dplyr_4 <- function(Users, Posts){
  #' funkcja z pakietu dplyr dla zadania 4
  # czesc I
  Posts_sub4 <- Posts %>% filter(PostTypeId == 2)
  AnsCount4 <- Posts_sub4 %>% group_by(ParentId) %>% summarise(AnswersCount = length(ParentId))
  # czesc II
  PostAuth4 <- inner_join(x = Posts, y = AnsCount4,
                          by = c("Id" = "ParentId"))
  
  PostAuth4 <- PostAuth4 %>% select(c("AnswersCount", "Id", "OwnerUserId"))
  
  res4 <- inner_join(x = Users, y = PostAuth4,
                     by = c("AccountId" = "OwnerUserId"))
  
  res4 <- res4 %>% group_by(AccountId, DisplayName, Location ) %>% summarise(AverageAnswersCount = mean(AnswersCount, na.rm = TRUE), .groups = "drop")
  # czesc III
  res4 <- res4 %>% arrange(-AverageAnswersCount, AccountId) %>% 
    slice_head(n = 10) 
}


df_table_4 <- function(Users,Posts){
  #' funkcja z pakietu data.table dla zadania 4
  UsersDT <- as.data.table(Users)
  PostsDT <- as.data.table(Posts)
  # czesc I
  Posts_sub4 <- PostsDT[PostTypeId == 2]
  AnsCount4 <- Posts_sub4[, .(AnswersCount = .N), by = .(ParentId)]
  # czesc II
  PostAuth4b <- merge(x = PostsDT, y = AnsCount4,
                      by.x = "Id", by.y = "ParentId")
  PostAuth4b <- PostAuth4b[, .(AnswersCount, Id, OwnerUserId)]
  res4b <- merge(x = UsersDT, y = PostAuth4b,
                 by.x = "AccountId", by.y = "OwnerUserId")
  res4b <- res4b[, .(AverageAnswersCount = mean(AnswersCount, na.rm = TRUE)), by = .(AccountId, DisplayName, Location)]
  # czesc III
  res4b <- res4b[order(-AverageAnswersCount, AccountId)][1:10]
}



df_sql_5 <- function(Votes, Posts){
  #' funkcja z pakietu sqldf dla zadania 5
  sql5 <- sqldf("
    SELECT Posts.Title, Posts.Id,
        STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date,
        VotesByAge.Votes
    FROM Posts
    JOIN (
        SELECT
            PostId,
            MAX(CASE WHEN VoteDate = 'new' THEN Total ELSE 0 END) NewVotes,
            MAX(CASE WHEN VoteDate = 'old' THEN Total ELSE 0 END) OldVotes,
            SUM(Total) AS Votes
        FROM (
            SELECT
                PostId,
                CASE STRFTIME('%Y', CreationDate)
                    WHEN '2021' THEN 'new'
                    WHEN '2020' THEN 'new'
                    ELSE 'old'
                    END VoteDate,
                COUNT(*) AS Total
            FROM Votes
            WHERE VoteTypeId IN (1, 2, 5)
            GROUP BY PostId, VoteDate
        ) AS VotesDates
        GROUP BY VotesDates.PostId
        HAVING NewVotes > OldVotes
    ) AS VotesByAge ON Posts.Id = VotesByAge.PostId
    WHERE Title NOT IN ('')
    ORDER BY Votes DESC
    LIMIT 10
")
}


df_base_5 <- function(Votes, Posts){
  #' funkcja z bazowego R dla zadania 5
  # czesc 1
  VotesDates <- Votes[Votes$VoteTypeId %in% c(1,2,5), ]
  years <- stri_sub(  VotesDates[,c("CreationDate")],1,4)
  res <- ifelse(years %in% c(2020,2021),"new", "old" ) #jesli rok to 2020 lub 2021 to wektor ma 'new'
  VotesDates[, "VoteDate"] <- res
  VotesDates <- aggregate(x = VotesDates$VoteDate, 
                          by = VotesDates[, c("PostId", "VoteDate")], FUN = length)
  names(VotesDates)
  names(VotesDates)[3] <- "Total"
  # czesc 2
  merge(
    merge( data.frame(PostId = unique(VotesDates$PostId)),
           VotesDates[VotesDates$VoteDate == 'new', c(1, 3)], all.x = TRUE
    ),
    VotesDates[VotesDates$VoteDate == 'old', c(1, 3)], 
    by = 'PostId', 
    suffixes = c("new", "old"), 
    all.x = TRUE
  ) -> VotesByAge 
  #podwojne mergowanie, ktore powoduje ze zamiast 2 wierszy z tym samym post id, zmieniam na jeden wiersz ale dodaje dodatkowa kolumne
  #bardzo podobnie mozna wykorzystac funkcje reshape ze stats
  VotesByAge[is.na(VotesByAge)] <- 0
  names(VotesByAge)[2] <- "NewVotes"
  names(VotesByAge)[3] <- "OldVotes"
  VotesByAge["Votes"] <- VotesByAge[, "NewVotes"] + VotesByAge[, "OldVotes"]
  VotesByAge <- VotesByAge[order(VotesByAge$PostId),]
  VotesByAge <- VotesByAge[VotesByAge$NewVotes > VotesByAge$OldVotes, ]
  # czesc 3
  res <- merge(x = Posts, y = VotesByAge,
               by.x = "Id", by.y = "PostId")
  #analogicznie jak w zadaniu 3 wyciagam date, czyli pierwsze 10 znakow
  res[, "Date"] <- stri_sub(  res[,c("CreationDate")],1,10)
  res <- res[, c("Title", "Id", "Date", "Votes")]
  res <- res[res$Title != "", ]
  res <- res[order(-res$Votes), ][1:10,]
}

df_dplyr_5 <- function(Votes, Posts){
  #' funkcja z pakietu dplyr dla zadania 5
  
  # 1 czesc
  VotesDatesDPL <- Votes %>% 
    filter(VoteTypeId %in% c(1,2,5))
  years <- stri_sub(VotesDatesDPL %>% pull(CreationDate) ,1,4)
  res2 <- case_when(years %in% c(2020,2021) ~ "new",
                    TRUE ~ "old")
  VotesDatesDPL <- VotesDatesDPL %>% 
    mutate(VoteDate = res2) %>% 
    group_by(PostId, VoteDate) %>% summarise( Total = length(VoteDate), .groups = "drop")
  #czesc 2
  VotesByAgeDP <- full_join(full_join(unique(VotesDatesDPL %>% select(c(1))),
                                      VotesDatesDPL %>% filter(VoteDate == "new") %>% select(c(1,3)), by = "PostId"),
                            VotesDatesDPL %>% filter(VoteDate == "old") %>% select(c(1,3)), 
                            by = "PostId")
  names(VotesByAgeDP)[c(2,3)] <- c("NewVotes", "OldVotes")
  VotesByAgeDP <- VotesByAgeDP %>% 
    mutate_all(~replace(., is.na(.), 0)) %>%  #wszystkie NA zastepuje 0
    mutate(Votes = NewVotes + OldVotes)  %>% 
    filter(NewVotes > OldVotes)
  # czesc 3
  resDP <- inner_join(x = Posts, y = VotesByAgeDP,
                      by = c("Id" = "PostId"))
  DateDP <- stri_sub(  resDP %>% pull(CreationDate), 1, 10)
  resDP %>% mutate(Date = DateDP) %>% 
    select(c("Title", "Id", "Date", "Votes") ) %>% 
    filter(Title != "")  %>% 
    arrange(-Votes) %>% 
    slice_head(n = 10)
}


df_table_5 <- function(Votes, Posts){
  #' funkcja z pakietu data.table dla zadania 5
  VotesDT <- as.data.table(Votes)
  PostsDT <- as.data.table(Posts)
  # czesc 1
  VotesDatesA <- VotesDT[VoteTypeId %in% c(1,2,5)]
  years <- stri_sub(  VotesDatesA[,CreationDate],1,4 )
  resA <- ifelse(years %in% c(2020,2021),"new", "old" )
  VotesDatesA <- VotesDatesA[, `:=`(VoteDate=resA)]
  VotesDatesA <- VotesDatesA[,.(Total = .N), by = .(PostId, VoteDate)]
  # czesc 2
  merge(
    merge(data.table(PostId = unique(VotesDatesA$PostId)),
          VotesDatesA[VoteDate == 'new', c(1, 3)], all.x = TRUE
    ),
    VotesDatesA[VoteDate == 'old', c(1, 3)], by = 'PostId', all.x = TRUE
  ) -> VotesByAgeA
  VotesByAgeA[is.na(VotesByAgeA)] <- 0
  names(VotesByAgeA)[2] <- "NewVotes"
  names(VotesByAgeA)[3] <- "OldVotes"
  VotesByAgeA <- VotesByAgeA[, `:=`(Votes = VotesByAgeA[,NewVotes] + VotesByAgeA[,OldVotes])]
  VotesByAgeA <- VotesByAgeA[NewVotes > OldVotes]
  # czesc 3
  resA <- merge(x = PostsDT, y = VotesByAgeA,
                by.x = "Id", by.y = "PostId")
  DateA <- stri_sub(  resA[,CreationDate],1,10)
  resA[, `:=`(Date = DateA)]
  resA <- resA[, .(Title,Id,Date,Votes)]
  resA <- resA[Title != ""]
  resA <- resA[order(-Votes)][1:10]
}
