-- BÀI 1: Sơ chế dữ liệu văn bản (Tiền xử lý)


-- 1. Load dữ liệu đánh giá khách sạn (ngăn cách bởi dấu ';')
raw_review = LOAD 'hotel-review.csv' USING PigStorage(';') AS (id:chararray, text:chararray, aspect:chararray, category:chararray, sentiment:chararray);

-- 2. Chuyển đổi tất cả ký tự về chữ thường (lowercase)
clean_review = FOREACH raw_review GENERATE id, aspect, category, sentiment, LOWER(text) AS text_lower;

-- 3. Tách các dòng bình luận thành dãy các từ dựa trên khoảng trắng (TOKENIZE)
tokenized_words = FOREACH clean_review GENERATE id, aspect, category, sentiment, FLATTEN(TOKENIZE(text_lower)) AS word;

-- 4. Load danh sách stop word
stopwords = LOAD 'stopwords.txt' AS (stopword:chararray);

-- 5. Loại bỏ các stop word sử dụng LEFT OUTER JOIN
joined_words = JOIN tokenized_words BY word LEFT OUTER, stopwords BY stopword;
filtered_words = FILTER joined_words BY stopwords::stopword IS NULL;

-- Giữ lại các trường dữ liệu sạch sau khi loại bỏ stopword để làm các bài tiếp theo
words = FOREACH filtered_words GENERATE tokenized_words::id AS id, tokenized_words::aspect AS aspect, tokenized_words::category AS category, tokenized_words::sentiment AS sentiment, tokenized_words::word AS word;
-- BÀI 2: Thống kê tần số

--- 2.1: Thống kê tần số xuất hiện của các từ và lọc các từ xuất hiện > 500 lần ---
grouped_all_words = GROUP words BY word;
word_counts = FOREACH grouped_all_words GENERATE group AS word, COUNT(words) AS count;
high_freq_words = FILTER word_counts BY count > 500;
STORE high_freq_words INTO 'output_bai2_1' USING PigStorage(';');

--- 2.2: Thống kê số bình luận theo từng phân loại (category) ---
grouped_category = GROUP raw_review BY category;
category_counts = FOREACH grouped_category GENERATE group AS category, COUNT(raw_review) AS total_comments;
STORE category_counts INTO 'output_bai2_2' USING PigStorage(';');

--- 2.3: Thống kê số bình luận theo từng khía cạnh đánh giá (aspect) ---
grouped_aspect = GROUP raw_review BY aspect;
aspect_counts = FOREACH grouped_aspect GENERATE group AS aspect, COUNT(raw_review) AS total_comments;
STORE aspect_counts INTO 'output_bai2_3' USING PigStorage(';');
-- BÀI 3: Xác định khía cạnh (Aspect) tích cực và tiêu cực nhất

-- Lọc dữ liệu theo sắc thái tích cực và tiêu cực
positive_reviews = FILTER raw_review BY sentiment == 'positive';
negative_reviews = FILTER raw_review BY sentiment == 'negative';

-- Khía cạnh nhận nhiều đánh giá TÍCH CỰC nhất
pos_aspect_group = GROUP positive_reviews BY aspect;
pos_aspect_counts = FOREACH pos_aspect_group GENERATE group AS aspect, COUNT(positive_reviews) AS total;
pos_aspect_sorted = ORDER pos_aspect_counts BY total DESC;
top1_positive_aspect = LIMIT pos_aspect_sorted 1;
STORE top1_positive_aspect INTO 'output_bai3_positive' USING PigStorage(';');

-- Khía cạnh nhận nhiều đánh giá TIÊU CỰC nhất
neg_aspect_group = GROUP negative_reviews BY aspect;
neg_aspect_counts = FOREACH neg_aspect_group GENERATE group AS aspect, COUNT(negative_reviews) AS total;
neg_aspect_sorted = ORDER neg_aspect_counts BY total DESC;
top1_negative_aspect = LIMIT neg_aspect_sorted 1;
STORE top1_negative_aspect INTO 'output_bai3_negative' USING PigStorage(';');-- BÀI 4: Tìm 5 từ tích cực/tiêu cực nhất theo từng phân loại (Category)

--- Ý 4.1: Tìm 5 từ mang ý nghĩa tích cực nhất theo từng phân loại ---
pos_words_only = FILTER words BY sentiment == 'positive';
cat_word_pos_group = GROUP pos_words_only BY (category, word);
cat_word_pos_counts = FOREACH cat_word_pos_group GENERATE group.category AS category, group.word AS word, COUNT(pos_words_only) AS count;

cat_pos_group = GROUP cat_word_pos_counts BY category;
top5_pos_words_per_cat = FOREACH cat_pos_group {
    sorted = ORDER cat_word_pos_counts BY count DESC;
    top5 = LIMIT sorted 5;
    GENERATE group AS category, top5.(word, count);
};
STORE top5_pos_words_per_cat INTO 'output_bai4_positive' USING PigStorage(';');

--- Ý 4.2: Tìm 5 từ mang ý nghĩa tiêu cực nhất theo từng phân loại ---
neg_words_only = FILTER words BY sentiment == 'negative';
cat_word_neg_group = GROUP neg_words_only BY (category, word);
cat_word_neg_counts = FOREACH cat_word_neg_group GENERATE group.category AS category, group.word AS word, COUNT(neg_words_only) AS count;

cat_neg_group = GROUP cat_word_neg_counts BY category;
top5_neg_words_per_cat = FOREACH cat_neg_group {
    sorted = ORDER cat_word_neg_counts BY count DESC;
    top5 = LIMIT sorted 5;
    GENERATE group AS category, top5.(word, count);
};
STORE top5_neg_words_per_cat INTO 'output_bai4_negative' USING PigStorage(';');
-- BÀI 5: Xác định 5 từ liên quan nhất (xuất hiện nhiều nhất) theo từng phân loại

cat_word_all_group = GROUP words BY (category, word);
cat_word_all_counts = FOREACH cat_word_all_group GENERATE group.category AS category, group.word AS word, COUNT(words) AS count;

cat_all_group = GROUP cat_word_all_counts BY category;
top5_relevant_words = FOREACH cat_all_group {
    sorted = ORDER cat_word_all_counts BY count DESC;
    top5 = LIMIT sorted 5;
    GENERATE group AS category, top5.(word, count);
};
STORE top5_relevant_words INTO 'output_bai5' USING PigStorage(';');