نشر نظام القوة النسبية المصري على Railway

1) على Railway أنشئي مشروعًا وPostgreSQL وVolume على مسار /data مرتبطًا بخدمة الويب.
2) يجب نقل جدولَي public.eg_tv_stocks_1d و public.eg_tv_indices_1d من قاعدة stocks_local المحلية إلى قاعدة Railway PostgreSQL (دون حذف النسخة المحلية).
3) اربطي متغير DATABASE_URL بخدمة PostgreSQL باستخدام Reference Variable، مثل ${{Postgres.DATABASE_URL}} حسب اسم الخدمة.
4) أضيفي DATA_DIR=/data لخدمة الويب.
5) ارفعي محتويات هذا المجلد عبر GitHub إلى خدمة Railway؛ Dockerfile سيشغّل Gunicorn ويستمع إلى PORT.
6) اختبري / و /api/update/availability، ثم شغّلي فحص CCAP.
7) لا تجعلي المشروع عامًا قبل إضافة تسجيل دخول وصلاحيات؛ أدوات تحديث البيانات والفحص متاحة عبر المسارات الحالية بلا حماية للمستخدمين.
8) التخزين الدائم للنتائج والمجموعات في /data عبر volume، لكن بيانات PostgreSQL تحتاج أيضًا نسخًا احتياطيًا مستقلاً.
9) ملف tvdatafeed يثبّت من GitHub، وهو غير رسمي وقد يتوقف أو يحتاج تعديلًا لاحقًا.
10) النتائج الافتراضية للواجهة تظل RS=29 / MA Fast=18 / MA Slow=29.

هام: اختبار بناء Docker والاتصال المباشر بقاعدة Railway لم يُنفذا هنا.
