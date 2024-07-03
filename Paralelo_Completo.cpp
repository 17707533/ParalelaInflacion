#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <chrono>
#include <map>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <omp.h>

using namespace std;

struct producto {
    string year;
    string month;
    string day;
    string sku;
    float quantity;
    float amount;
};

void extractDateParts(const string& created, string& year, string& month, string& day) {
    if (created.size() >= 10) {
        year = created.substr(0, 4);
        month = created.substr(5, 2);
        day = created.substr(8, 2);
    } else {
        year = month = day = "-1";
    }
}

bool isLaterDate(const string& date1, const string& date2) {
    return date1.compare(date2) > 0;
}

map<pair<string, string>, map<string, producto>> procesar_datos(const string& filename) {
    map<pair<string, string>, map<string, producto>> lastDayProducts;
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "No se pudo abrir el archivo " << filename << endl;
        return lastDayProducts;
    }

    string line;
    getline(file, line); // Saltar encabezado

    int chunkSize = 1000; // Tamaño del bloque a procesar

    while (file) {
        vector<string> lines;
        lines.reserve(chunkSize);

        for (int i = 0; i < chunkSize && getline(file, line); ++i) {
            lines.push_back(line);
        }

        if (lines.empty()) {
            break;
        }

        #pragma omp parallel
        {
            map<pair<string, string>, map<string, producto>> localLastDayProducts;

            #pragma omp for schedule(static)
            for (int i = 0; i < lines.size(); ++i) {
                stringstream ss(lines[i]);
                string value;
                vector<string> values;
                while (getline(ss, value, ';')) {
                    values.push_back(value);
                }

                if (values.size() < 10) continue;

                producto temp;
                for (auto& val : values) {
                    if (!val.empty()) {
                        val = val.substr(1, val.size() - 2);
                    }
                }

                string created = values[0];
                extractDateParts(created, temp.year, temp.month, temp.day);

                temp.sku = values[6];
                try {
                    temp.quantity = stof(values[7]);
                } catch (const invalid_argument&) {
                    temp.quantity = -1.0;
                }

                try {
                    temp.amount = stof(values[9]);
                } catch (const invalid_argument&) {
                    temp.amount = 1.0;
                }

                auto yearMonthKey = make_pair(temp.year, temp.month);
                auto& skuMap = localLastDayProducts[yearMonthKey];

                if (skuMap.find(temp.sku) == skuMap.end() ||
                    isLaterDate(temp.day, skuMap[temp.sku].day)) {
                    skuMap[temp.sku] = temp;
                }
            }

            #pragma omp critical
            {
                for (const auto& yearMonthEntry : localLastDayProducts) {
                    for (const auto& skuEntry : yearMonthEntry.second) {
                        const auto& prod = skuEntry.second;
                        auto& globalSkuMap = lastDayProducts[yearMonthEntry.first];

                        if (globalSkuMap.find(prod.sku) == globalSkuMap.end() ||
                            isLaterDate(prod.day, globalSkuMap[prod.sku].day)) {
                            globalSkuMap[prod.sku] = prod;
                        }
                    }
                }
            }
        }
    }

    return lastDayProducts;
}

map<pair<string, string>, map<string, producto>> procesar_datos_conversion(const string& filename) {
    map<pair<string, string>, map<string, producto>> lastDayProducts;
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "No se pudo abrir el archivo " << filename << endl;
        return lastDayProducts;
    }

    string line;
    getline(file, line); // Saltar encabezado

    while (getline(file, line)) {
        stringstream ss(line);
        vector<string> values;
        string value;
        
        while (getline(ss, value, ',')) {
            values.push_back(value);
        }

        if (values.size() < 6) continue;

        producto temp;
        temp.year = values[0];
        temp.month = values[1];
        temp.day = values[2];
        temp.sku = values[3];
        try {
            temp.quantity = stof(values[4]);
        } catch (const invalid_argument&) {
            temp.quantity = -1.0;
        }

        try {
            temp.amount = stof(values[5]);
        } catch (const invalid_argument&) {
            temp.amount = 1.0;
        }

        auto yearMonthKey = make_pair(temp.year, temp.month);
        auto& skuMap = lastDayProducts[yearMonthKey];

        if (skuMap.find(temp.sku) == skuMap.end() || isLaterDate(temp.day, skuMap[temp.sku].day)) {
            skuMap[temp.sku] = temp;
        }
    }

    return lastDayProducts;
}

float calcular_promedio(const vector<float>& prices) {
    if (prices.empty()) {
        return 0.0;
    }
    float sum = accumulate(prices.begin(), prices.end(), 0.0f);
    return sum / prices.size();
}

map<pair<string, string>, map<string, producto>> leer_datos(const string& filename) {
    map<pair<string, string>, map<string, producto>> data;
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "No se pudo abrir el archivo " << filename << endl;
        return data;
    }

    string line;
    getline(file, line); // Saltar encabezado

    while (getline(file, line)) {
        stringstream ss(line);
        string value;
        vector<string> values;
        while (getline(ss, value, ',')) {
            values.push_back(value);
        }

        if (values.size() < 6) continue;

        producto temp;
        temp.year = values[0];
        temp.month = values[1];
        temp.day = values[2];
        temp.sku = values[3];
        temp.quantity = stof(values[4]);
        temp.amount = stof(values[5]);

        auto yearMonthKey = make_pair(temp.year, temp.month);
        auto& skuMap = data[yearMonthKey];
        skuMap[temp.sku] = temp;
    }

    return data;
}

void imprimir_resultados(const string& pais, const map<pair<string, string>, map<string, producto>>& lastDayProducts) {
    map<pair<string, string>, vector<float>> precios_por_mes;

    for (const auto& yearMonthEntry : lastDayProducts) {
        for (const auto& skuEntry : yearMonthEntry.second) {
            const auto& prod = skuEntry.second;
            auto key = make_pair(prod.year, prod.month);
            precios_por_mes[key].push_back(prod.amount);
        }
    }

    map<string, vector<float>> precios_por_ano;
    vector<pair<string, string>> keys;
    for (const auto& entry : precios_por_mes) {
        keys.push_back(entry.first);
    }

    #pragma omp parallel
    {
        map<string, vector<float>> local_precios_por_ano;

        #pragma omp for nowait
        for (size_t i = 0; i < keys.size(); ++i) {
            const auto& year = keys[i].first;
            const auto& prices = precios_por_mes[keys[i]];
            local_precios_por_ano[year].insert(local_precios_por_ano[year].end(), prices.begin(), prices.end());
        }

        #pragma omp critical
        {
            for (const auto& entry : local_precios_por_ano) {
                precios_por_ano[entry.first].insert(precios_por_ano[entry.first].end(),
                                                    entry.second.begin(), entry.second.end());
            }
        }
    }

    map<string, float> promedio_anual;
    #pragma omp parallel for
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& year = keys[i].first;
        const auto& prices = precios_por_ano[year];
        float promedio_precio = calcular_promedio(prices);

        #pragma omp critical
        promedio_anual[year] = promedio_precio;
    }

    cout << "IPC (Indice de Precios al Consumidor) - " << pais << ":" << endl;
    for (const auto& entry : promedio_anual) {
        const auto& year = entry.first;
        float promedio_precio = entry.second;
        cout << year << ": " << fixed << setprecision(2) << promedio_precio << endl;
    }

    cout << "\nInflacion anual (%) - " << pais << ":" << endl;
    string previous_year = "";
    for (const auto& entry : promedio_anual) {
        const auto& current_year = entry.first;
        if (!previous_year.empty()) {
            float inflation = ((promedio_anual[previous_year] - promedio_anual[current_year]) / promedio_anual[previous_year]) * 100;
            cout << previous_year << " a " << current_year << ": " << fixed << setprecision(2) << inflation << "%" << endl;
        }
        previous_year = current_year;
    }
}

int main() {
    auto start_total = chrono::high_resolution_clock::now(); // Iniciar el temporizador total

    const string inputFilenamePeru = "pd.csv";
    const string outputFilenamePeru = "dataloca.csv";
    const string inputFilenameChile = "dataloca_conversion.csv";

    // Procesar y guardar los datos de Perú
    auto lastDayProductsPeru = procesar_datos(inputFilenamePeru);

    ofstream outputFilePeru(outputFilenamePeru);
    if (!outputFilePeru.is_open()) {
        cerr << "No se pudo abrir el archivo de salida " << outputFilenamePeru << endl;
        return 1;
    }

    outputFilePeru << "year,month,day,sku,quantity,amount\n";

    for (const auto& yearMonthEntry : lastDayProductsPeru) {
        for (const auto& skuEntry : yearMonthEntry.second) {
            const auto& prod = skuEntry.second;
            outputFilePeru << prod.year << ","
                           << prod.month << ","
                           << prod.day << ","
                           << prod.sku << ","
                           << prod.quantity << ","
                           << prod.amount << "\n";
        }
    }

    outputFilePeru.close();

    // Leer datos para Perú desde el archivo guardado
    auto processedDataPeru = leer_datos(outputFilenamePeru);

    // Leer y procesar los datos de Chile directamente
    auto lastDayProductsChile = procesar_datos_conversion(inputFilenameChile);

    cout << "Resultados para Perú:" << endl;
    imprimir_resultados("Perú", processedDataPeru);

    cout << "\nResultados para Chile:" << endl;
    imprimir_resultados("Chile", lastDayProductsChile);

    auto end_total = chrono::high_resolution_clock::now(); // Finalizar el temporizador total
    double tiempoTotal = chrono::duration_cast<chrono::duration<double>>(end_total - start_total).count(); // Calcular tiempo total
    cout << "Tiempo total de procesamiento: " << tiempoTotal << " segundos" << endl;

    return 0;
}
