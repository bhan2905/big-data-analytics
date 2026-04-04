-- Load data 
data = LOAD 'lab2/input/hotel-review.csv' USING PigStorage(';') AS (
    id: int, review: chararray, category: chararray, aspect: chararray, sentiment: chararray
);

-- Task 1: Do some simple normalization on text data

-- Lowercase the review text
lower_data = FOREACH data GENERATE
    LOWER(review) AS review,
    category AS category,
    aspect AS aspect,
    sentiment AS sentiment;

-- Remove all non-letter characters
clean_data = FOREACH lower_data GENERATE
    REPLACE(review, '[^\\p{L}\\s]', ' ') AS review,
    category AS category,
    aspect AS aspect,
    sentiment AS sentiment; 

-- Tokernize the review text by splitting on whitespace
words = FOREACH clean_data GENERATE
    FLATTEN(TOKENIZE(review)) AS word,
    category AS category,
    aspect AS aspect,
    sentiment AS sentiment;

-- Remove stop words
stopwords = LOAD 'lab2/input/stopwords.txt' USING PigStorage() AS (word: chararray);
words = JOIN words BY word LEFT OUTER, stopwords BY word;
words = FILTER words BY stopwords::word IS NULL;

words = FOREACH words GENERATE
    words::word AS word,
    words::category AS category,
    words::aspect AS aspect,
    words::sentiment AS sentiment;

-- Task 2: Do some statistical analysis on the clean data

-- Count the frequency of each words
word_group = GROUP words BY word;
word_freq = FOREACH word_group GENERATE
    group AS word,
    COUNT (words) AS frequency;

-- Filter wwords that appear more than 500 times
word_over_500 = FILTER word_freq BY frequency > 500;

-- Count the number of reviews by category
category_group = GROUP clean_data BY category;
reviews_category = FOREACH category_group GENERATE
    group AS category,
    COUNT (clean_data) AS review_count;

-- Count the number of reviews by aspect
aspect_group = GROUP clean_data BY  aspect;
reviews_aspect = FOREACH aspect_group GENERATE
    group AS aspect,
    COUNT (clean_data) AS review_count;

-- Count the number of reviews by category and aspect
cat_asp_group = GROUP clean_data BY (category, aspect);
reviews_cat_asp = FOREACH cat_asp_group GENERATE
    group.$0 AS category,
    group.$1 AS aspect,
    COUNT (clean_data) AS review_count;

-- Task 3: Define the aspects with the most positive and negative sentiments

-- Filter positive and negative reviews
pos_reviews = FILTER clean_data BY sentiment == 'positive';
neg_reviews = FILTER clean_data BY sentiment == 'negative';

-- Count the number of positive and negative reviews by aspect
pos_aspect_group = GROUP pos_reviews BY aspect;
neg_aspect_group = GROUP neg_reviews BY aspect;

pos_aspect_count = FOREACH pos_aspect_group GENERATE
    group AS aspect,
    COUNT (pos_reviews) AS count;

neg_aspect_count = FOREACH neg_aspect_group GENERATE
    group AS aspect,
    COUNT (neg_reviews) AS count;

-- Define the aspects with the most positive and negative sentiments
pos_aspect = ORDER pos_aspect_count BY count DESC;
ranked_pos = RANK pos_aspect;
most_pos_aspects = FILTER ranked_pos BY rank_pos_aspect == 1; -- Get the aspect with the highest count of positive reviews

neg_aspect = ORDER neg_aspect_count BY count DESC;
ranked_neg = RANK neg_aspect;
most_neg_aspects = FILTER ranked_neg BY rank_neg_aspect == 1; -- Get the aspect with the highest count of negative reviews

most_pos_aspects = FOREACH most_pos_aspects GENERATE 'positive' AS sentiment, aspect, count AS count;
most_neg_aspects = FOREACH most_neg_aspects GENERATE 'negative' AS sentiment, aspect, count AS count;

most_aspects = UNION most_pos_aspects, most_neg_aspects;

-- Task 4: Basing on the category, find 5 words that are the most positive and 5 words that are the most negative
-- Filter the sentiment words
pos_words = FILTER words BY sentiment == 'positive';
neg_words = FILTER words BY sentiment == 'negative';

-- Count the frequency of the sentiment words by category
pos_word_group = GROUP pos_words BY (category, word);
neg_word_group = GROUP neg_words BY (category, word);

pos_word_freq = FOREACH pos_word_group GENERATE
    group.$0 AS category,
    group.$1 AS word,
    COUNT (pos_words) AS frequency;

neg_word_freq = FOREACH neg_word_group GENERATE
    group.$0 AS category,
    group.$1 AS word,
    COUNT (neg_words) AS frequency;

-- Get the top 5 positive and negative words by category
grouped_pos_words = GROUP pos_word_freq BY category;
top_pos_words = FOREACH grouped_pos_words {
    word_sort = ORDER pos_word_freq BY frequency DESC;
    top5 = LIMIT word_sort 5;
    GENERATE group AS category, FLATTEN(top5.(word, frequency));
};

grouped_neg_words = GROUP neg_word_freq BY category;
top_neg_words = FOREACH grouped_neg_words {
    word_sort = ORDER neg_word_freq BY frequency DESC;
    top5 = LIMIT word_sort 5;
    GENERATE group AS category, FLATTEN(top5.(word, frequency));
};

-- Task 5: Basing on the category, find 5 words that are the most relative to category

-- Count the frequency of each word by category
word_group = GROUP words BY (category, word);
word_freq = FOREACH word_group GENERATE
    group.$0 AS category,
    group.$1 AS word,
    COUNT(words) AS frequency;

-- Get top 5 words relative to category
grouped_words = GROUP word_freq BY category;

top_relative_words = FOREACH grouped_words {
    word_sort = ORDER word_freq BY frequency DESC;
    top5 = LIMIT word_sort 5;
    GENERATE group AS category, FLATTEN(top5.(word, frequency));
};

-- Save output with tab separator
STORE top_relative_words INTO 'lab2/output' USING PigStorage('\t');

-- Show results
DUMP top_relative_words;