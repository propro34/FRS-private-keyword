# FRS Private Keyword v1.2.1 — إصلاح النشر

## سبب الخطأ
كان Render يعرض `ENOENT: no such file or directory .../public/index.html`، وهذا يعني أن ملف الواجهة لم يصل إلى المستودع في المسار المتوقع.

## الملفات
- `server.js` يدعم الآن `public/index.html` أو `index.html` في جذر المشروع.
- أضفنا `index.html` في الجذر لتسهيل الرفع اليدوي على GitHub من الهاتف.
- أبقينا `public/index.html` أيضًا.

## النشر
ارفع محتويات هذا المجلد إلى جذر مستودع `FRS-private-keyword`، مع الحفاظ على مجلد `public`. تأكد أن الملفات تظهر هكذا:

```text
server.js
package.json
index.html
public/index.html
README_AR.md
```

Build Command: `npm install`
Start Command: `npm start`

بعد الرفع نفّذ Manual Deploy. إذا ظهر الخطأ نفسه، افتح تبويب Files في GitHub وتأكد أن `public/index.html` أو `index.html` ظاهر فعلًا في المستودع.
