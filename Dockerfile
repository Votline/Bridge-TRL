FROM archlinux:latest AS builder
WORKDIR /app

RUN pacman -Syu --noconfirm \
    go git gcc pkg-config \
    tesseract leptonica \
    opus opusfile portaudio \
    && pacman -Scc --noconfirm


RUN git clone --depth=1 --filter=blob:none --sparse \
    https://github.com/Votline/EasyTranslate.git /tmp/easytranslate && \
    cd /tmp/easytranslate && \
    git sparse-checkout set dicts && \
    cp -r dicts /app/dicts

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=1
ENV CGO_CFLAGS="-I/app/assets/vosk-linux-x86_64-0.3.45 -I/usr/include"
ENV CGO_LDFLAGS="-L/app/assets/vosk-linux-x86_64-0.3.45 -L/app/libs -lvosk -ltesseract -llept -lopus -lportaudio"

RUN go build -o bridge_trl main.go

FROM archlinux:latest
WORKDIR /app

RUN pacman -Syu --noconfirm \
    tesseract \
    tesseract-data-rus \
    opus opusfile \
    portaudio \
    rhvoice \
    rhvoice-language-russian \
    rhvoice-voice-anna \
    && pacman -Scc --noconfirm

# ВОТ ЭТО ТЫ ПОТЕРЯЛ: Хак для языка и переменная
RUN ln -s /usr/share/tessdata/rus.traineddata /usr/share/tessdata/ru.traineddata
ENV TESSDATA_PREFIX=/usr/share/tessdata/

COPY --from=builder /app/bridge_trl .
COPY --from=builder /app/libs libs
COPY --from=builder /app/dicts dicts
COPY --from=builder /app/assets assets

ENV LD_LIBRARY_PATH="assets/vosk-linux-x86_64-0.3.45:libs:/usr/lib:$LD_LIBRARY_PATH"

CMD ["./bridge_trl"]
