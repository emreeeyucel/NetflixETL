# Netflix ETL Projesi

Bu proje, Netflix'teki TV şovları, filmler ve oyuncular hakkındaki verileri analiz etmek, temizlemek ve MongoDB veritabanına yüklemek amacıyla geliştirilmiştir. Python ve MongoDB kullanılarak yapılmış olan ETL (Extract, Transform, Load) işlemleri, verileri gruplamak, filtrelemek ve analiz etmek için kullanılmıştır.

## Proje İçeriği

Projenin amacı, Netflix veri setindeki önemli bilgileri işleyerek aşağıdaki analizleri yapmaktır:

1. **Veri Temizleme:** Verideki eksik (null) değerler "Unkown" ile doldurulmuştur.
2. **Yönetmenlerin TV Şovları:** Hangi yönetmenin daha çok TV şovu yaptığı ve bu şovların hangi yıllarda yayımlandığı analiz edilmiştir.
3. **Ülkeler Bazında Oyuncu Sayıları:** Her ülkenin sahip olduğu farklı oyuncu sayıları belirlenmiş ve sıralama yapılmıştır.
4. **Oyuncuların En Çok Çalıştığı Yönetmenler:** Oyuncuların, yıl bazında en çok çalıştıkları yönetmenler analiz edilmiştir.
5. **MongoDB İle Veri Yükleme:** Tüm veriler MongoDB veritabanına yüklenmiş ve burada saklanmıştır.

## Kullanılan Teknolojiler

- **Python**: Veri analizi, işleme ve ETL işlemleri için kullanılmıştır.
- **Pandas**: Veri manipülasyonu ve analizi için kullanılmıştır.
- **MongoDB**: Verilerin saklandığı veritabanıdır.
- **CSV**: Veri seti, CSV formatında bulunmaktadır.


