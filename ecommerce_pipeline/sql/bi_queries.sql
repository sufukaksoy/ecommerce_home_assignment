-- =========================================================================
-- BI EKİBİ İÇİN ÖRNEK SORGULAR
-- Bu dosya, BI araçlarının (Tableau, PowerBI vb.) oluşturulan analitik
-- view'ları nasıl tüketeceğini gösteren referans sorgularını içerir.
-- =========================================================================

-- 1. Günlük Ciro ve Sipariş Hacmi Analizi (Daily Revenue & Order Volume)
-- Amaç: Şirketin gün bazındaki toplam satış performansını izlemek.
SELECT 
    order_date, 
    daily_order_volume AS "Toplam Sipariş", 
    daily_revenue AS "Net Ciro ($)"
FROM mart.daily_summary_metrics
ORDER BY order_date DESC
LIMIT 10;

-- 2. Kategorilere Göre Günlük Satış Analizi (Category Performance)
-- Amaç: Hangi kategorinin hangi gün ne kadar sattığını ve ciro getirdiğini görmek.
SELECT 
    order_date, 
    category AS "Kategori", 
    units_sold AS "Satılan Ürün Adedi", 
    category_revenue AS "Kategori Cirosu ($)"
FROM mart.daily_commerce_metrics
ORDER BY order_date DESC, category_revenue DESC
LIMIT 10;

-- 3. Günün En Çok Satan Kategorisi (Top Selling Product Category per Day)
-- Amaç: "Günün Şampiyonu" olan kategoriyi dashboard'un en tepesinde göstermek.
SELECT 
    order_date, 
    category AS "Günün Lider Kategorisi", 
    category_revenue AS "Elde Edilen Ciro ($)"
FROM mart.daily_top_category
ORDER BY order_date DESC
LIMIT 10;
