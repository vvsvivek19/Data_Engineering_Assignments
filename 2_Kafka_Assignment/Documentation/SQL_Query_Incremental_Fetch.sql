SELECT id,name,category,price,last_updated
from products
WHERE last_updated > last_processed_timestamp
ORDER BY last_updated;