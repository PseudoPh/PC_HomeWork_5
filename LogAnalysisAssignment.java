import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/*
 * ПРАКТИЧЕСКОЕ ЗАДАНИЕ: МНОГОПОТОЧНЫЙ АНАЛИЗ ЛОГОВ ВЕБ-СЕРВЕРА
 *
 * Крупные части задания:
 * 1. Сгенерировать набор настоящих файлов с логами.
 * 2. Реализовать разбор строк и последовательный отчёт.
 * 3. Распараллелить обработку нескольких файлов через ExecutorService.
 * 4. Сделать общую статистику потокобезопасной.
 * 5. Ограничить число одновременно читаемых файлов через Semaphore.
 * 6. Добавить Atomic-счётчики, CAS и корректное ожидание завершения.
 * 7. Реализовать досрочную остановку анализа через volatile.
 * 8. Сформулировать и доказать инварианты корректности и отсутствия дедлоков.
 * 9. Реализовать обработку повреждённых файлов и частичный отчёт.
 *
 * ФАЙЛ МОЖНО ЗАПУСТИТЬ САМОСТОЯТЕЛЬНО:
 *   java LogAnalysisAssignment generate logs 20 1000
 *   java LogAnalysisAssignment check
 *
 * Первая команда создаёт каталог logs и файлы access-000.log, access-001.log и т.д.
 * Аргументы: каталог, количество файлов, количество строк в каждом файле.
 * Вторая команда выполняет встроенную проверку генератора и итератора строк.
 *
 * ФОРМАТ СТРОКИ:
 *   METHOD PATH STATUS RESPONSE_BYTES DURATION_MILLIS
 * Например:
 *   GET /api/users 200 820 4
 *
 * Поля разделены одним или несколькими пробелами.
 * Успешный запрос имеет статус от 200 до 399 включительно.
 * Ошибочный запрос имеет статус от 400 и выше.
 *
 * ГОТОВЫЕ СТРУКТУРЫ:
 *   LogEntry       — одна разобранная запись.
 *   LogFile        — путь к одному файлу.
 *   PathStatistics — неизменяемая статистика одного URL.
 *   Report         — итоговый отчёт.
 *   LogFileLines   — итератор строк файла.
 *   GenerateLogs   — генератор входных файлов.
 *
 * ВАЖНО ПРО LogFileLines:
 * Итератор потоконебезопасен. Один экземпляр Iterator<String> нельзя читать
 * из нескольких потоков одновременно. Каждый поток должен получить собственный
 * итератор для своего файла. Нельзя разделить один итератор между задачами.
 * Сам файл можно читать параллельно разными потоками только при условии, что
 * каждый поток создаёт отдельный итератор.
 *
 * ЧАСТИ ЗАДАНИЯ:
 *
 * 1. РАЗБОР СТРОКИ
 * Реализуйте преобразование одной строки в LogEntry. Проверьте количество
 * полей, преобразуйте числовые значения и выбрасывайте понятную ошибку для
 * повреждённой строки.
 *
 * 2. ПОСЛЕДОВАТЕЛЬНЫЙ ОТЧЁТ
 * Напишите обработку всех файлов одним потоком. Для каждого URL собирайте
 * количество запросов, количество успешных запросов и ошибок, объём ответов,
 * суммарное и максимальное время обработки. Этот вариант станет эталоном для
 * проверки многопоточной версии.
 *
 * 3. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ФАЙЛОВ
 * Создайте фиксированный ExecutorService. Каждый файл обрабатывается отдельной
 * задачей. Рабочий поток создаёт собственный LogFileLines.Iterator, читает свой
 * файл и передаёт записи в общий аккумулятор. Главный поток должен дождаться
 * завершения всех файлов и только потом построить Report.
 *
 * 4. ПОТОКОБЕЗОПАСНАЯ СТАТИСТИКА
 * Реализуйте аккумулятор статистики. Сначала допустимо использовать один общий
 * synchronized-метод. Затем реализуйте вариант с отдельным synchronized-аккумулятором
 * для каждого URL и ConcurrentHashMap. Сравните эти варианты на данных, где почти
 * все запросы относятся к одному URL, и на данных с большим числом URL.
 *
 * 5. ОГРАНИЧЕНИЕ ЧТЕНИЯ ФАЙЛОВ
 * Добавьте Semaphore, ограничивающий число файлов, которые могут обрабатываться
 * одновременно. Рабочих потоков может быть больше разрешений семафора. Разрешение
 * нужно получить перед чтением файла и гарантированно вернуть после завершения.
 * Добавьте наблюдение за текущим и максимальным числом активных файлов.
 *
 * 6. ATOMIC-СЧЁТЧИКИ И CAS
 * Добавьте счётчики обработанных файлов, прочитанных строк, корректных записей,
 * повреждённых записей, ошибок чтения и активных файлов. Для максимального числа
 * активных файлов используйте AtomicInteger и compareAndSet. Обычные изменяемые
 * int/long для этих глобальных счётчиков не используйте.
 *
 * 7. ОЖИДАНИЕ ЗАВЕРШЕНИЯ
 * Организуйте ожидание всех файлов через CountDownLatch либо через набор Future.
 * Завершение задачи должно учитываться даже при исключении. ExecutorService нужно
 * корректно закрыть и дождаться его завершения.
 *
 * CountDownLatch — это одноразовый счётчик-сигнал. Его создают с начальным
 * числом, например равным количеству файлов. Главный поток вызывает await()
 * и блокируется, пока счётчик не станет равен нулю. Каждая завершившаяся задача
 * один раз вызывает countDown(), уменьшая счётчик. Когда последний счётчик
 * уменьшен до нуля, все ожидающие потоки продолжают работу. Увеличить latch
 * обратно или использовать его повторно нельзя, поэтому для нового запуска
 * создаётся новый объект. Вызов countDown() нужно помещать в finally, чтобы
 * исключение в задаче не оставило главный поток ждать бесконечно.
 *
 * 8. ДОСРОЧНАЯ ОСТАНОВКА
 * Добавьте volatile-флаг остановки. После остановки новые строки не начинают
 * обрабатываться, а уже начатая операция может завершиться. Опишите, какие данные
 * входят в частичный отчёт. Не оставляйте рабочие потоки и разрешения семафора
 * в подвешенном состоянии.
 *
 * 9. ПОВРЕЖДЁННЫЕ ФАЙЛЫ И ЧАСТИЧНЫЙ ОТЧЁТ
 * Сделайте так, чтобы ошибка одного файла не отменяла обработку остальных файлов.
 * В итоговом результате отдельно храните список или число файлов, которые не удалось
 * обработать. Решите, что происходит при повреждённой строке: прекращается обработка
 * всего файла или пропускается только эта строка. Выберите один вариант и явно
 * зафиксируйте его в документации. Для полного запуска корректные файлы должны
 * давать полный отчёт, а ошибка должна быть видна пользователю.
 *
 * ИНВАРИАНТЫ, КОТОРЫЕ НУЖНО ДОКАЗАТЬ:
 * - каждая строка полного запуска обработана ровно один раз;
 * - для каждого URL requests = successful + errors;
 * - maxDuration <= totalDuration при наличии запросов;
 * - 0 <= activeFiles <= semaphoreLimit;
 * - после завершения анализа activeFiles равно нулю;
 * - один повреждённый файл не мешает обработке остальных;
 * - в программе нет дедлока.
 *
 * В доказательстве отсутствия дедлока укажите порядок захвата мониторов и
 * семафоров. Отдельно объясните, может ли поток ждать acquire(), удерживая
 * synchronized-монитор, и почему разрешение семафора возвращается при исключении.
 * Объясните, почему volatile нужен для флага остановки.
 *
 * НЕБОЛЬШИЕ ЭКСПЕРИМЕНТЫ ПОСЛЕ РЕАЛИЗАЦИИ:
 * - сравнить 1 и 4 рабочих потока;
 * - сравнить лимит семафора 1 и 4;
 * - сравнить глобальную блокировку и блокировки по URL;
 * - добавить одну повреждённую строку или один повреждённый файл и проверить,
 *   что остальные данные всё равно попадают в отчёт.
 * Эксперименты не требуют дополнительной архитектуры или новых классов.
 */
public class LogAnalysisAssignment {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis
    ) {}

    /**
     * Возвращает отдельный итератор для чтения строк одного файла.
     *
     * ВАЖНО: Iterator<String> потоконебезопасен. Один экземпляр нельзя
     * одновременно читать из нескольких потоков. Для каждого потока нужен
     * отдельный вызов LogFileLines.iterator(path).
     */
    public static final class LogFileLines {
        private LogFileLines() {}

        public static Iterator<String> iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(
                        path,
                        StandardCharsets.UTF_8
                );

                return new Iterator<>() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException(
                                    "Cannot read " + path,
                                    exception
                            );
                        }
                    }

                    private void close() {
                        if (closed) {
                            return;
                        }
                        closed = true;
                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException(
                                    "Cannot close " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) {
                            throw new NoSuchElementException();
                        }
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException(
                        "Cannot open " + path,
                        exception
                );
            }
        }
    }

    /** Генерирует реальные текстовые файлы с детерминированными данными. */
    public static final class GenerateLogs {
        private static final String[] METHODS = {
                "GET", "POST", "PUT", "DELETE"
        };
        private static final String[] PATHS = {
                "/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"
        };
        private static final int[] STATUSES = {
                200, 200, 200, 201, 204,
                301, 400, 401, 404, 500
        };

        private GenerateLogs() {}

        public static void generate(
                Path outputDirectory,
                int fileCount,
                int linesPerFile
        ) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException(
                        "Counts must be non-negative"
                );
            }

            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve(
                        "access-%03d.log".formatted(fileId)
                );

                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8
                )) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method =
                                METHODS[random.nextInt(METHODS.length)];
                        String path =
                                PATHS[random.nextInt(PATHS.length)];
                        int status =
                                STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes =
                                100L + random.nextInt(100_000);
                        long durationMillis =
                                1L + random.nextInt(500);

                        writer.write(
                                "%s %s %d %d %d%n".formatted(
                                        method,
                                        path,
                                        status,
                                        responseBytes,
                                        durationMillis
                                )
                        );
                    }
                }
            }
        }
    }

    public static LogEntry parseLine(String line) {
        String[] parts = line.trim().split("\\s+");
        if (parts.length != 5) {
            throw new IllegalArgumentException(
                    "Expected 5 fields, got " + parts.length + " in line: " + line
            );
        }
        int status;
        long responseBytes;
        long durationMillis;
        try {
            status = Integer.parseInt(parts[2]);
            responseBytes = Long.parseLong(parts[3]);
            durationMillis = Long.parseLong(parts[4]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Not a number in line: " + line);
        }
        return new LogEntry(
                parts[0],
                parts[1],
                status,
                responseBytes,
                durationMillis
        );
    }

    static class PathCounter {
        long requests;
        long successful;
        long errors;
        long responseBytes;
        long totalDuration;
        long maxDuration;

        void add(LogEntry entry) {
            requests++;
            if (entry.status() >= 200 && entry.status() < 400) {
                successful++;
            } else {
                errors++;
            }
            responseBytes += entry.responseBytes();
            totalDuration += entry.durationMillis();
            if (entry.durationMillis() > maxDuration) {
                maxDuration = entry.durationMillis();
            }
        }

        PathStatistics toStatistics() {
            return new PathStatistics(
                    requests,
                    successful,
                    errors,
                    responseBytes,
                    totalDuration,
                    maxDuration
            );
        }
    }

    public static class GlobalLockAccumulator {
        private final Map<String, PathCounter> map =
                new HashMap<String, PathCounter>();

        public synchronized void add(LogEntry entry) {
            PathCounter counter = map.get(entry.path());
            if (counter == null) {
                counter = new PathCounter();
                map.put(entry.path(), counter);
            }
            counter.add(entry);
        }

        public synchronized Map<String, PathStatistics> toMap() {
            Map<String, PathStatistics> result =
                    new HashMap<String, PathStatistics>();
            for (Map.Entry<String, PathCounter> entry : map.entrySet()) {
                result.put(entry.getKey(), entry.getValue().toStatistics());
            }
            return result;
        }
    }

    public static class PerUrlAccumulator {
        private final ConcurrentHashMap<String, PathCounter> map =
                new ConcurrentHashMap<String, PathCounter>();

        public void add(LogEntry entry) {
            PathCounter counter = map.get(entry.path());
            if (counter == null) {
                PathCounter created = new PathCounter();
                PathCounter old = map.putIfAbsent(entry.path(), created);
                if (old == null) {
                    counter = created;
                } else {
                    counter = old;
                }
            }
            synchronized (counter) {
                counter.add(entry);
            }
        }

        public Map<String, PathStatistics> toMap() {
            Map<String, PathStatistics> result =
                    new HashMap<String, PathStatistics>();
            for (Map.Entry<String, PathCounter> entry : map.entrySet()) {
                result.put(entry.getKey(), entry.getValue().toStatistics());
            }
            return result;
        }
    }

    public static class AnalysisContext {
        final Semaphore semaphore;
        final GlobalLockAccumulator global = new GlobalLockAccumulator();
        final PerUrlAccumulator perUrl = new PerUrlAccumulator();
        final AtomicInteger processedFiles = new AtomicInteger();
        final AtomicLong readLines = new AtomicLong();
        final AtomicLong validEntries = new AtomicLong();
        final AtomicLong corruptedEntries = new AtomicLong();
        final AtomicInteger readErrors = new AtomicInteger();
        final AtomicInteger activeFiles = new AtomicInteger();
        final AtomicInteger maxActiveFiles = new AtomicInteger();
        final List<String> failedFiles =
                Collections.synchronizedList(new ArrayList<String>());
        long stopAfterLines;
        private volatile boolean stopped;

        public AnalysisContext(int semaphoreLimit) {
            semaphore = new Semaphore(semaphoreLimit);
        }

        public void stop() {
            stopped = true;
        }

        public boolean isStopped() {
            return stopped;
        }

        void updateMaxActive(int value) {
            int current = maxActiveFiles.get();
            while (value > current) {
                if (maxActiveFiles.compareAndSet(current, value)) {
                    return;
                }
                current = maxActiveFiles.get();
            }
        }
    }

    public static class AnalysisResult {
        public final Report report;
        public final Map<String, PathStatistics> paths;
        public final List<String> failedFiles;
        public final int processedFiles;
        public final long readLines;
        public final long validEntries;
        public final long corruptedEntries;
        public final int readErrors;
        public final int maxActiveFiles;
        public final int activeFiles;
        public final boolean stopped;

        public AnalysisResult(
                Report report,
                Map<String, PathStatistics> paths,
                List<String> failedFiles,
                int processedFiles,
                long readLines,
                long validEntries,
                long corruptedEntries,
                int readErrors,
                int maxActiveFiles,
                int activeFiles,
                boolean stopped
        ) {
            this.report = report;
            this.paths = paths;
            this.failedFiles = failedFiles;
            this.processedFiles = processedFiles;
            this.readLines = readLines;
            this.validEntries = validEntries;
            this.corruptedEntries = corruptedEntries;
            this.readErrors = readErrors;
            this.maxActiveFiles = maxActiveFiles;
            this.activeFiles = activeFiles;
            this.stopped = stopped;
        }
    }

    static Report buildReport(Map<String, PathStatistics> paths) {
        long totalRequests = 0;
        long successfulRequests = 0;
        long errorRequests = 0;
        long responseBytes = 0;
        long maxDuration = 0;
        for (PathStatistics statistics : paths.values()) {
            totalRequests += statistics.requests();
            successfulRequests += statistics.successful();
            errorRequests += statistics.errors();
            responseBytes += statistics.responseBytes();
            if (statistics.maxDurationMillis() > maxDuration) {
                maxDuration = statistics.maxDurationMillis();
            }
        }
        return new Report(
                totalRequests,
                successfulRequests,
                errorRequests,
                responseBytes,
                maxDuration
        );
    }

    static void checkInvariants(Map<String, PathStatistics> paths) {
        boolean ok = true;
        for (Map.Entry<String, PathStatistics> entry : paths.entrySet()) {
            PathStatistics statistics = entry.getValue();
            if (statistics.requests()
                    != statistics.successful() + statistics.errors()) {
                ok = false;
                System.out.println(
                        "  requests != successful + errors: " + entry.getKey()
                );
            }
            if (statistics.requests() > 0
                    && statistics.maxDurationMillis()
                    > statistics.totalDurationMillis()) {
                ok = false;
                System.out.println(
                        "  maxDuration > totalDuration: " + entry.getKey()
                );
            }
        }
        if (ok) {
            System.out.println("  invariants per URL: OK");
        }
    }

    static boolean sameStatistics(PathStatistics a, PathStatistics b) {
        return a.requests() == b.requests()
                && a.successful() == b.successful()
                && a.errors() == b.errors()
                && a.responseBytes() == b.responseBytes()
                && a.totalDurationMillis() == b.totalDurationMillis()
                && a.maxDurationMillis() == b.maxDurationMillis();
    }

    static boolean compareReports(AnalysisResult a, AnalysisResult b) {
        if (a.report.totalRequests() != b.report.totalRequests()
                || a.report.successfulRequests() != b.report.successfulRequests()
                || a.report.errorRequests() != b.report.errorRequests()
                || a.report.responseBytes() != b.report.responseBytes()
                || a.report.maxDurationMillis() != b.report.maxDurationMillis()) {
            return false;
        }
        if (a.paths.size() != b.paths.size()) {
            return false;
        }
        for (Map.Entry<String, PathStatistics> entry : a.paths.entrySet()) {
            PathStatistics second = b.paths.get(entry.getKey());
            if (second == null) {
                return false;
            }
            if (!sameStatistics(entry.getValue(), second)) {
                return false;
            }
        }
        return true;
    }

    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);

        int files = 0;
        int lines = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) {
                        throw new AssertionError(
                                "Unexpected line: " + line
                        );
                    }
                    Integer.parseInt(fields[2]);
                    Long.parseLong(fields[3]);
                    Long.parseLong(fields[4]);
                    lines++;
                }
            }
        }

        if (files != 3 || lines != 21) {
            throw new AssertionError(
                    "Expected 3 files and 21 lines, got "
                            + files + " files and " + lines + " lines"
            );
        }

        System.out.println(
                "OK: generated " + files
                        + " files and read " + lines + " lines"
        );
    }

    static AnalysisResult finish(AnalysisContext context, boolean useGlobalLock) {
        Map<String, PathStatistics> paths;
        if (useGlobalLock) {
            paths = context.global.toMap();
        } else {
            paths = context.perUrl.toMap();
        }
        Report report = buildReport(paths);
        List<String> failed = new ArrayList<String>(context.failedFiles);
        return new AnalysisResult(
                report,
                paths,
                failed,
                context.processedFiles.get(),
                context.readLines.get(),
                context.validEntries.get(),
                context.corruptedEntries.get(),
                context.readErrors.get(),
                context.maxActiveFiles.get(),
                context.activeFiles.get(),
                context.isStopped()
        );
    }

    public static AnalysisResult analyzeSequential(List<LogFile> files) {
        AnalysisContext context = new AnalysisContext(1);
        for (LogFile file : files) {
            try {
                Iterator<String> iterator = LogFileLines.iterator(file.path());
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    context.readLines.incrementAndGet();
                    try {
                        LogEntry entry = parseLine(line);
                        context.global.add(entry);
                        context.validEntries.incrementAndGet();
                    } catch (IllegalArgumentException e) {
                        context.corruptedEntries.incrementAndGet();
                    }
                }
                context.processedFiles.incrementAndGet();
            } catch (RuntimeException e) {
                context.readErrors.incrementAndGet();
                context.failedFiles.add(file.path().toString());
                System.out.println("  cannot read: " + file.path());
            }
        }
        return finish(context, true);
    }

    static void readOneFile(
            LogFile file,
            AnalysisContext context,
            boolean useGlobalLock
    ) {
        boolean permit = false;
        boolean complete = false;
        try {
            if (context.isStopped()) {
                return;
            }
            context.semaphore.acquire();
            permit = true;
            context.updateMaxActive(context.activeFiles.incrementAndGet());
            try {
                Iterator<String> iterator = LogFileLines.iterator(file.path());
                while (iterator.hasNext()) {
                    if (context.isStopped()) {
                        return;
                    }
                    String line = iterator.next();
                    long read = context.readLines.incrementAndGet();
                    if (context.stopAfterLines > 0
                            && read >= context.stopAfterLines) {
                        context.stop();
                        return;
                    }
                    try {
                        LogEntry entry = parseLine(line);
                        if (useGlobalLock) {
                            context.global.add(entry);
                        } else {
                            context.perUrl.add(entry);
                        }
                        context.validEntries.incrementAndGet();
                    } catch (IllegalArgumentException e) {
                        context.corruptedEntries.incrementAndGet();
                    }
                }
                complete = true;
            } finally {
                context.activeFiles.decrementAndGet();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            context.readErrors.incrementAndGet();
            context.failedFiles.add(file.path().toString());
        } catch (RuntimeException e) {
            context.readErrors.incrementAndGet();
            context.failedFiles.add(file.path().toString());
            System.out.println("  cannot read: " + file.path());
        } finally {
            if (permit) {
                context.semaphore.release();
            }
            if (complete) {
                context.processedFiles.incrementAndGet();
            }
        }
    }

    public static AnalysisResult analyzeParallel(
            List<LogFile> files,
            int threads,
            int semaphoreLimit,
            boolean useGlobalLock,
            long stopAfterLines
    ) throws InterruptedException {
        if (threads <= 0) {
            throw new IllegalArgumentException("Threads must be positive");
        }
        if (semaphoreLimit <= 0) {
            throw new IllegalArgumentException("Semaphore limit must be positive");
        }

        final AnalysisContext context = new AnalysisContext(semaphoreLimit);
        context.stopAfterLines = stopAfterLines;
        final boolean global = useGlobalLock;
        final ExecutorService executor = Executors.newFixedThreadPool(threads);
        final CountDownLatch latch = new CountDownLatch(files.size());

        for (final LogFile file : files) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        readOneFile(file, context, global);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        try {
            latch.await(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            context.stop();
            executor.shutdownNow();
            throw e;
        }
        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }
        return finish(context, global);
    }

    static List<LogFile> listLogs(Path directory) throws IOException {
        List<LogFile> files = new ArrayList<LogFile>();
        File[] children = directory.toFile().listFiles();
        if (children == null) {
            return files;
        }
        for (File child : children) {
            if (child.getName().endsWith(".log")) {
                files.add(new LogFile(child.toPath()));
            }
        }
        Collections.sort(
                files,
                new Comparator<LogFile>() {
                    @Override
                    public int compare(LogFile a, LogFile b) {
                        return a.path().toString()
                                .compareTo(b.path().toString());
                    }
                }
        );
        return files;
    }

    static void copyWithPaths(Path source, Path target, String mode)
            throws IOException {
        Files.createDirectories(target);
        File[] children = source.toFile().listFiles();
        if (children == null) {
            return;
        }
        for (File child : children) {
            List<String> lines = Files.readAllLines(
                    child.toPath(),
                    StandardCharsets.UTF_8
            );
            List<String> result = new ArrayList<String>();
            for (int i = 0; i < lines.size(); i++) {
                String[] parts = lines.get(i).trim().split("\\s+");
                if (parts.length != 5) {
                    result.add(lines.get(i));
                    continue;
                }
                if (mode.equals("single")) {
                    parts[1] = "/api/users";
                } else {
                    parts[1] = "/api/item-" + (i % 40);
                }
                result.add(
                        parts[0] + " " + parts[1] + " " + parts[2] + " "
                                + parts[3] + " " + parts[4]
                );
            }
            Files.write(
                    target.resolve(child.getName()),
                    result,
                    StandardCharsets.UTF_8
            );
        }
    }

    static void prepareBroken(Path source, Path target) throws IOException {
        Files.createDirectories(target);
        File[] children = source.toFile().listFiles();
        if (children == null) {
            return;
        }
        int index = 0;
        for (File child : children) {
            List<String> lines = Files.readAllLines(
                    child.toPath(),
                    StandardCharsets.UTF_8
            );
            if (index == 0 && !lines.isEmpty()) {
                lines.set(0, "GET /api/users 200 abc 5");
            }
            if (index == 1 && lines.size() > 4) {
                lines.set(4, "POST /login");
            }
            if (index == 2 && lines.size() > 6) {
                lines.set(6, "GET /search 200 12345 77 extra-field");
            }
            Files.write(
                    target.resolve(child.getName()),
                    lines,
                    StandardCharsets.UTF_8
            );
            index++;
        }
        Files.createDirectories(target.resolve("access-999.log"));
    }

    static String pad(String value, int width) {
        StringBuilder builder = new StringBuilder(value);
        while (builder.length() < width) {
            builder.append(' ');
        }
        return builder.toString();
    }

    static void printResult(String title, AnalysisResult result) {
        System.out.println(title);
        System.out.println(
                "  requests: " + result.report.totalRequests()
                        + ", successful: " + result.report.successfulRequests()
                        + ", errors: " + result.report.errorRequests()
        );
        System.out.println(
                "  bytes: " + result.report.responseBytes()
                        + ", max duration: " + result.report.maxDurationMillis()
                        + " ms"
        );
        System.out.println(
                "  files: " + result.processedFiles
                        + ", lines: " + result.readLines
                        + ", valid: " + result.validEntries
                        + ", corrupted: " + result.corruptedEntries
        );
        System.out.println(
                "  read errors: " + result.readErrors
                        + ", max active files: " + result.maxActiveFiles
                        + ", stopped: " + result.stopped
        );
        if (!result.failedFiles.isEmpty()) {
            System.out.println("  failed: " + result.failedFiles);
        }
    }

    static void printPaths(AnalysisResult result) {
        List<String> keys = new ArrayList<String>(result.paths.keySet());
        Collections.sort(keys);
        System.out.println(
                "  " + pad("URL", 22) + pad("req", 8) + pad("ok", 8)
                        + pad("err", 8) + pad("bytes", 12)
                        + pad("total ms", 10) + "max ms"
        );
        for (String key : keys) {
            PathStatistics statistics = result.paths.get(key);
            System.out.println(
                    "  " + pad(key, 22)
                            + pad(String.valueOf(statistics.requests()), 8)
                            + pad(String.valueOf(statistics.successful()), 8)
                            + pad(String.valueOf(statistics.errors()), 8)
                            + pad(String.valueOf(statistics.responseBytes()), 12)
                            + pad(String.valueOf(
                            statistics.totalDurationMillis()), 10)
                            + statistics.maxDurationMillis()
            );
        }
    }

    private static void runExperiments() throws Exception {
        Path dir = Path.of("logs");
        if (!Files.exists(dir)) {
            GenerateLogs.generate(dir, 20, 1000);
        }
        List<LogFile> files = listLogs(dir);
        System.out.println("files in " + dir + ": " + files.size());

        long start = System.currentTimeMillis();
        AnalysisResult sequential = analyzeSequential(files);
        long spent = System.currentTimeMillis() - start;
        printResult("sequential, " + spent + " ms", sequential);
        printPaths(sequential);
        checkInvariants(sequential.paths);

        start = System.currentTimeMillis();
        AnalysisResult oneThread = analyzeParallel(files, 1, 1, false, 0);
        System.out.println(
                "\n1 thread, limit 1: " + (System.currentTimeMillis() - start)
                        + " ms, same as sequential: "
                        + compareReports(sequential, oneThread)
        );

        start = System.currentTimeMillis();
        AnalysisResult fourThreads = analyzeParallel(files, 4, 4, false, 0);
        System.out.println(
                "4 threads, limit 4: " + (System.currentTimeMillis() - start)
                        + " ms, same as sequential: "
                        + compareReports(sequential, fourThreads)
        );

        start = System.currentTimeMillis();
        AnalysisResult onePermit = analyzeParallel(files, 4, 1, false, 0);
        System.out.println(
                "4 threads, limit 1: " + (System.currentTimeMillis() - start)
                        + " ms, max active files: " + onePermit.maxActiveFiles
        );

        start = System.currentTimeMillis();
        AnalysisResult eightThreads = analyzeParallel(files, 8, 4, false, 0);
        System.out.println(
                "8 threads, limit 4: " + (System.currentTimeMillis() - start)
                        + " ms, max active files: " + eightThreads.maxActiveFiles
        );

        Path singleDir = Path.of("logs_single");
        if (!Files.exists(singleDir)) {
            copyWithPaths(dir, singleDir, "single");
        }
        List<LogFile> singleFiles = listLogs(singleDir);

        Path manyDir = Path.of("logs_many");
        if (!Files.exists(manyDir)) {
            copyWithPaths(dir, manyDir, "many");
        }
        List<LogFile> manyFiles = listLogs(manyDir);

        start = System.currentTimeMillis();
        AnalysisResult globalSingle = analyzeParallel(singleFiles, 4, 4, true, 0);
        System.out.println(
                "\none URL, global lock: "
                        + (System.currentTimeMillis() - start) + " ms"
        );

        start = System.currentTimeMillis();
        AnalysisResult perUrlSingle = analyzeParallel(singleFiles, 4, 4, false, 0);
        System.out.println(
                "one URL, per-URL lock: "
                        + (System.currentTimeMillis() - start) + " ms"
        );
        System.out.println(
                "  reports match: " + compareReports(globalSingle, perUrlSingle)
        );

        start = System.currentTimeMillis();
        AnalysisResult globalMany = analyzeParallel(manyFiles, 4, 4, true, 0);
        System.out.println(
                "many URLs, global lock: "
                        + (System.currentTimeMillis() - start) + " ms"
        );

        start = System.currentTimeMillis();
        AnalysisResult perUrlMany = analyzeParallel(manyFiles, 4, 4, false, 0);
        System.out.println(
                "many URLs, per-URL lock: "
                        + (System.currentTimeMillis() - start) + " ms"
        );
        System.out.println(
                "  reports match: " + compareReports(globalMany, perUrlMany)
                        + ", URLs in run: " + perUrlMany.paths.size()
        );

        Path brokenDir = Path.of("logs_broken");
        if (!Files.exists(brokenDir)) {
            prepareBroken(dir, brokenDir);
        }
        List<LogFile> brokenFiles = listLogs(brokenDir);
        AnalysisResult broken = analyzeParallel(brokenFiles, 4, 2, false, 0);
        printResult("\nbroken data", broken);
        System.out.println(
                "  requests lost against sequential: "
                        + (sequential.report.totalRequests()
                        - broken.report.totalRequests())
        );

        AnalysisResult stopped = analyzeParallel(files, 4, 4, false, 5000);
        printResult("\nstop after 5000 lines", stopped);
        System.out.println(
                "  stop flag set: " + stopped.stopped
                        + ", active files after finish: " + stopped.activeFiles
        );
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            int fileCount = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 20;
            int linesPerFile = args.length > 3
                    ? Integer.parseInt(args[3])
                    : 1_000;

            GenerateLogs.generate(
                    directory,
                    fileCount,
                    linesPerFile
            );

            System.out.println(
                    "Generated " + fileCount
                            + " files in " + directory
            );
            return;
        }

        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }

        if (args.length > 0 && args[0].equals("experiment")) {
            runExperiments();
            return;
        }

        System.out.println(
                "Usage: java LogAnalysisAssignment "
                        + "generate <directory> <files> <lines>"
        );
        System.out.println(
                "       java LogAnalysisAssignment check"
        );
        System.out.println(
                "       java LogAnalysisAssignment experiment"
        );
    }
}
