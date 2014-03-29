/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.expr.fn.impl;

import java.util.HashMap;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimeFormatterBuilder;

// Utility class for Date, DateTime, TimeStamp, Interval data types
public class DateUtility {


    /* We have a hashmap that stores the timezone as the key and an index as the value
     * While storing the timezone in value vectors, holders we only use this index. As we
     * reconstruct the timestamp, we use this index to index through the array timezoneList
     * and get the corresponding timezone and pass it to joda-time
     */
    public static HashMap<String, Integer> timezoneMap = new HashMap<String, Integer>();

    public static String[] timezoneList =  {"Africa/Abidjan",
                                            "Africa/Accra",
                                            "Africa/Addis_Ababa",
                                            "Africa/Algiers",
                                            "Africa/Asmara",
                                            "Africa/Asmera",
                                            "Africa/Bamako",
                                            "Africa/Bangui",
                                            "Africa/Banjul",
                                            "Africa/Bissau",
                                            "Africa/Blantyre",
                                            "Africa/Brazzaville",
                                            "Africa/Bujumbura",
                                            "Africa/Cairo",
                                            "Africa/Casablanca",
                                            "Africa/Ceuta",
                                            "Africa/Conakry",
                                            "Africa/Dakar",
                                            "Africa/Dar_es_Salaam",
                                            "Africa/Djibouti",
                                            "Africa/Douala",
                                            "Africa/El_Aaiun",
                                            "Africa/Freetown",
                                            "Africa/Gaborone",
                                            "Africa/Harare",
                                            "Africa/Johannesburg",
                                            "Africa/Juba",
                                            "Africa/Kampala",
                                            "Africa/Khartoum",
                                            "Africa/Kigali",
                                            "Africa/Kinshasa",
                                            "Africa/Lagos",
                                            "Africa/Libreville",
                                            "Africa/Lome",
                                            "Africa/Luanda",
                                            "Africa/Lubumbashi",
                                            "Africa/Lusaka",
                                            "Africa/Malabo",
                                            "Africa/Maputo",
                                            "Africa/Maseru",
                                            "Africa/Mbabane",
                                            "Africa/Mogadishu",
                                            "Africa/Monrovia",
                                            "Africa/Nairobi",
                                            "Africa/Ndjamena",
                                            "Africa/Niamey",
                                            "Africa/Nouakchott",
                                            "Africa/Ouagadougou",
                                            "Africa/Porto-Novo",
                                            "Africa/Sao_Tome",
                                            "Africa/Timbuktu",
                                            "Africa/Tripoli",
                                            "Africa/Tunis",
                                            "Africa/Windhoek",
                                            "America/Adak",
                                            "America/Anchorage",
                                            "America/Anguilla",
                                            "America/Antigua",
                                            "America/Araguaina",
                                            "America/Argentina/Buenos_Aires",
                                            "America/Argentina/Catamarca",
                                            "America/Argentina/ComodRivadavia",
                                            "America/Argentina/Cordoba",
                                            "America/Argentina/Jujuy",
                                            "America/Argentina/La_Rioja",
                                            "America/Argentina/Mendoza",
                                            "America/Argentina/Rio_Gallegos",
                                            "America/Argentina/Salta",
                                            "America/Argentina/San_Juan",
                                            "America/Argentina/San_Luis",
                                            "America/Argentina/Tucuman",
                                            "America/Argentina/Ushuaia",
                                            "America/Aruba",
                                            "America/Asuncion",
                                            "America/Atikokan",
                                            "America/Atka",
                                            "America/Bahia",
                                            "America/Bahia_Banderas",
                                            "America/Barbados",
                                            "America/Belem",
                                            "America/Belize",
                                            "America/Blanc-Sablon",
                                            "America/Boa_Vista",
                                            "America/Bogota",
                                            "America/Boise",
                                            "America/Buenos_Aires",
                                            "America/Cambridge_Bay",
                                            "America/Campo_Grande",
                                            "America/Cancun",
                                            "America/Caracas",
                                            "America/Catamarca",
                                            "America/Cayenne",
                                            "America/Cayman",
                                            "America/Chicago",
                                            "America/Chihuahua",
                                            "America/Coral_Harbour",
                                            "America/Cordoba",
                                            "America/Costa_Rica",
                                            "America/Cuiaba",
                                            "America/Curacao",
                                            "America/Danmarkshavn",
                                            "America/Dawson",
                                            "America/Dawson_Creek",
                                            "America/Denver",
                                            "America/Detroit",
                                            "America/Dominica",
                                            "America/Edmonton",
                                            "America/Eirunepe",
                                            "America/El_Salvador",
                                            "America/Ensenada",
                                            "America/Fort_Wayne",
                                            "America/Fortaleza",
                                            "America/Glace_Bay",
                                            "America/Godthab",
                                            "America/Goose_Bay",
                                            "America/Grand_Turk",
                                            "America/Grenada",
                                            "America/Guadeloupe",
                                            "America/Guatemala",
                                            "America/Guayaquil",
                                            "America/Guyana",
                                            "America/Halifax",
                                            "America/Havana",
                                            "America/Hermosillo",
                                            "America/Indiana/Indianapolis",
                                            "America/Indiana/Knox",
                                            "America/Indiana/Marengo",
                                            "America/Indiana/Petersburg",
                                            "America/Indiana/Tell_City",
                                            "America/Indiana/Vevay",
                                            "America/Indiana/Vincennes",
                                            "America/Indiana/Winamac",
                                            "America/Indianapolis",
                                            "America/Inuvik",
                                            "America/Iqaluit",
                                            "America/Jamaica",
                                            "America/Jujuy",
                                            "America/Juneau",
                                            "America/Kentucky/Louisville",
                                            "America/Kentucky/Monticello",
                                            "America/Knox_IN",
                                            "America/Kralendijk",
                                            "America/La_Paz",
                                            "America/Lima",
                                            "America/Los_Angeles",
                                            "America/Louisville",
                                            "America/Lower_Princes",
                                            "America/Maceio",
                                            "America/Managua",
                                            "America/Manaus",
                                            "America/Marigot",
                                            "America/Martinique",
                                            "America/Matamoros",
                                            "America/Mazatlan",
                                            "America/Mendoza",
                                            "America/Menominee",
                                            "America/Merida",
                                            "America/Metlakatla",
                                            "America/Mexico_City",
                                            "America/Miquelon",
                                            "America/Moncton",
                                            "America/Monterrey",
                                            "America/Montevideo",
                                            "America/Montreal",
                                            "America/Montserrat",
                                            "America/Nassau",
                                            "America/New_York",
                                            "America/Nipigon",
                                            "America/Nome",
                                            "America/Noronha",
                                            "America/North_Dakota/Beulah",
                                            "America/North_Dakota/Center",
                                            "America/North_Dakota/New_Salem",
                                            "America/Ojinaga",
                                            "America/Panama",
                                            "America/Pangnirtung",
                                            "America/Paramaribo",
                                            "America/Phoenix",
                                            "America/Port-au-Prince",
                                            "America/Port_of_Spain",
                                            "America/Porto_Acre",
                                            "America/Porto_Velho",
                                            "America/Puerto_Rico",
                                            "America/Rainy_River",
                                            "America/Rankin_Inlet",
                                            "America/Recife",
                                            "America/Regina",
                                            "America/Resolute",
                                            "America/Rio_Branco",
                                            "America/Rosario",
                                            "America/Santa_Isabel",
                                            "America/Santarem",
                                            "America/Santiago",
                                            "America/Santo_Domingo",
                                            "America/Sao_Paulo",
                                            "America/Scoresbysund",
                                            "America/Shiprock",
                                            "America/Sitka",
                                            "America/St_Barthelemy",
                                            "America/St_Johns",
                                            "America/St_Kitts",
                                            "America/St_Lucia",
                                            "America/St_Thomas",
                                            "America/St_Vincent",
                                            "America/Swift_Current",
                                            "America/Tegucigalpa",
                                            "America/Thule",
                                            "America/Thunder_Bay",
                                            "America/Tijuana",
                                            "America/Toronto",
                                            "America/Tortola",
                                            "America/Vancouver",
                                            "America/Virgin",
                                            "America/Whitehorse",
                                            "America/Winnipeg",
                                            "America/Yakutat",
                                            "America/Yellowknife",
                                            "Antarctica/Casey",
                                            "Antarctica/Davis",
                                            "Antarctica/DumontDUrville",
                                            "Antarctica/Macquarie",
                                            "Antarctica/Mawson",
                                            "Antarctica/McMurdo",
                                            "Antarctica/Palmer",
                                            "Antarctica/Rothera",
                                            "Antarctica/South_Pole",
                                            "Antarctica/Syowa",
                                            "Antarctica/Vostok",
                                            "Arctic/Longyearbyen",
                                            "Asia/Aden",
                                            "Asia/Almaty",
                                            "Asia/Amman",
                                            "Asia/Anadyr",
                                            "Asia/Aqtau",
                                            "Asia/Aqtobe",
                                            "Asia/Ashgabat",
                                            "Asia/Ashkhabad",
                                            "Asia/Baghdad",
                                            "Asia/Bahrain",
                                            "Asia/Baku",
                                            "Asia/Bangkok",
                                            "Asia/Beirut",
                                            "Asia/Bishkek",
                                            "Asia/Brunei",
                                            "Asia/Calcutta",
                                            "Asia/Choibalsan",
                                            "Asia/Chongqing",
                                            "Asia/Chungking",
                                            "Asia/Colombo",
                                            "Asia/Dacca",
                                            "Asia/Damascus",
                                            "Asia/Dhaka",
                                            "Asia/Dili",
                                            "Asia/Dubai",
                                            "Asia/Dushanbe",
                                            "Asia/Gaza",
                                            "Asia/Harbin",
                                            "Asia/Hebron",
                                            "Asia/Ho_Chi_Minh",
                                            "Asia/Hong_Kong",
                                            "Asia/Hovd",
                                            "Asia/Irkutsk",
                                            "Asia/Istanbul",
                                            "Asia/Jakarta",
                                            "Asia/Jayapura",
                                            "Asia/Jerusalem",
                                            "Asia/Kabul",
                                            "Asia/Kamchatka",
                                            "Asia/Karachi",
                                            "Asia/Kashgar",
                                            "Asia/Kathmandu",
                                            "Asia/Katmandu",
                                            "Asia/Kolkata",
                                            "Asia/Krasnoyarsk",
                                            "Asia/Kuala_Lumpur",
                                            "Asia/Kuching",
                                            "Asia/Kuwait",
                                            "Asia/Macao",
                                            "Asia/Macau",
                                            "Asia/Magadan",
                                            "Asia/Makassar",
                                            "Asia/Manila",
                                            "Asia/Muscat",
                                            "Asia/Nicosia",
                                            "Asia/Novokuznetsk",
                                            "Asia/Novosibirsk",
                                            "Asia/Omsk",
                                            "Asia/Oral",
                                            "Asia/Phnom_Penh",
                                            "Asia/Pontianak",
                                            "Asia/Pyongyang",
                                            "Asia/Qatar",
                                            "Asia/Qyzylorda",
                                            "Asia/Rangoon",
                                            "Asia/Riyadh",
                                            "Asia/Saigon",
                                            "Asia/Sakhalin",
                                            "Asia/Samarkand",
                                            "Asia/Seoul",
                                            "Asia/Shanghai",
                                            "Asia/Singapore",
                                            "Asia/Taipei",
                                            "Asia/Tashkent",
                                            "Asia/Tbilisi",
                                            "Asia/Tehran",
                                            "Asia/Tel_Aviv",
                                            "Asia/Thimbu",
                                            "Asia/Thimphu",
                                            "Asia/Tokyo",
                                            "Asia/Ujung_Pandang",
                                            "Asia/Ulaanbaatar",
                                            "Asia/Ulan_Bator",
                                            "Asia/Urumqi",
                                            "Asia/Vientiane",
                                            "Asia/Vladivostok",
                                            "Asia/Yakutsk",
                                            "Asia/Yekaterinburg",
                                            "Asia/Yerevan",
                                            "Atlantic/Azores",
                                            "Atlantic/Bermuda",
                                            "Atlantic/Canary",
                                            "Atlantic/Cape_Verde",
                                            "Atlantic/Faeroe",
                                            "Atlantic/Faroe",
                                            "Atlantic/Jan_Mayen",
                                            "Atlantic/Madeira",
                                            "Atlantic/Reykjavik",
                                            "Atlantic/South_Georgia",
                                            "Atlantic/St_Helena",
                                            "Atlantic/Stanley",
                                            "Australia/ACT",
                                            "Australia/Adelaide",
                                            "Australia/Brisbane",
                                            "Australia/Broken_Hill",
                                            "Australia/Canberra",
                                            "Australia/Currie",
                                            "Australia/Darwin",
                                            "Australia/Eucla",
                                            "Australia/Hobart",
                                            "Australia/LHI",
                                            "Australia/Lindeman",
                                            "Australia/Lord_Howe",
                                            "Australia/Melbourne",
                                            "Australia/NSW",
                                            "Australia/North",
                                            "Australia/Perth",
                                            "Australia/Queensland",
                                            "Australia/South",
                                            "Australia/Sydney",
                                            "Australia/Tasmania",
                                            "Australia/Victoria",
                                            "Australia/West",
                                            "Australia/Yancowinna",
                                            "Brazil/Acre",
                                            "Brazil/DeNoronha",
                                            "Brazil/East",
                                            "Brazil/West",
                                            "CET",
                                            "CST6CDT",
                                            "Canada/Atlantic",
                                            "Canada/Central",
                                            "Canada/East-Saskatchewan",
                                            "Canada/Eastern",
                                            "Canada/Mountain",
                                            "Canada/Newfoundland",
                                            "Canada/Pacific",
                                            "Canada/Saskatchewan",
                                            "Canada/Yukon",
                                            "Chile/Continental",
                                            "Chile/EasterIsland",
                                            "Cuba",
                                            "EET",
                                            "EST",
                                            "EST5EDT",
                                            "Egypt",
                                            "Eire",
                                            "Etc/GMT",
                                            "Etc/GMT+0",
                                            "Etc/GMT+1",
                                            "Etc/GMT+10",
                                            "Etc/GMT+11",
                                            "Etc/GMT+12",
                                            "Etc/GMT+2",
                                            "Etc/GMT+3",
                                            "Etc/GMT+4",
                                            "Etc/GMT+5",
                                            "Etc/GMT+6",
                                            "Etc/GMT+7",
                                            "Etc/GMT+8",
                                            "Etc/GMT+9",
                                            "Etc/GMT-0",
                                            "Etc/GMT-1",
                                            "Etc/GMT-10",
                                            "Etc/GMT-11",
                                            "Etc/GMT-12",
                                            "Etc/GMT-13",
                                            "Etc/GMT-14",
                                            "Etc/GMT-2",
                                            "Etc/GMT-3",
                                            "Etc/GMT-4",
                                            "Etc/GMT-5",
                                            "Etc/GMT-6",
                                            "Etc/GMT-7",
                                            "Etc/GMT-8",
                                            "Etc/GMT-9",
                                            "Etc/GMT0",
                                            "Etc/Greenwich",
                                            "Etc/UCT",
                                            "Etc/UTC",
                                            "Etc/Universal",
                                            "Etc/Zulu",
                                            "Europe/Amsterdam",
                                            "Europe/Andorra",
                                            "Europe/Athens",
                                            "Europe/Belfast",
                                            "Europe/Belgrade",
                                            "Europe/Berlin",
                                            "Europe/Bratislava",
                                            "Europe/Brussels",
                                            "Europe/Bucharest",
                                            "Europe/Budapest",
                                            "Europe/Chisinau",
                                            "Europe/Copenhagen",
                                            "Europe/Dublin",
                                            "Europe/Gibraltar",
                                            "Europe/Guernsey",
                                            "Europe/Helsinki",
                                            "Europe/Isle_of_Man",
                                            "Europe/Istanbul",
                                            "Europe/Jersey",
                                            "Europe/Kaliningrad",
                                            "Europe/Kiev",
                                            "Europe/Lisbon",
                                            "Europe/Ljubljana",
                                            "Europe/London",
                                            "Europe/Luxembourg",
                                            "Europe/Madrid",
                                            "Europe/Malta",
                                            "Europe/Mariehamn",
                                            "Europe/Minsk",
                                            "Europe/Monaco",
                                            "Europe/Moscow",
                                            "Europe/Nicosia",
                                            "Europe/Oslo",
                                            "Europe/Paris",
                                            "Europe/Podgorica",
                                            "Europe/Prague",
                                            "Europe/Riga",
                                            "Europe/Rome",
                                            "Europe/Samara",
                                            "Europe/San_Marino",
                                            "Europe/Sarajevo",
                                            "Europe/Simferopol",
                                            "Europe/Skopje",
                                            "Europe/Sofia",
                                            "Europe/Stockholm",
                                            "Europe/Tallinn",
                                            "Europe/Tirane",
                                            "Europe/Tiraspol",
                                            "Europe/Uzhgorod",
                                            "Europe/Vaduz",
                                            "Europe/Vatican",
                                            "Europe/Vienna",
                                            "Europe/Vilnius",
                                            "Europe/Volgograd",
                                            "Europe/Warsaw",
                                            "Europe/Zagreb",
                                            "Europe/Zaporozhye",
                                            "Europe/Zurich",
                                            "GB",
                                            "GB-Eire",
                                            "GMT",
                                            "GMT+0",
                                            "GMT-0",
                                            "GMT0",
                                            "Greenwich",
                                            "HST",
                                            "Hongkong",
                                            "Iceland",
                                            "Indian/Antananarivo",
                                            "Indian/Chagos",
                                            "Indian/Christmas",
                                            "Indian/Cocos",
                                            "Indian/Comoro",
                                            "Indian/Kerguelen",
                                            "Indian/Mahe",
                                            "Indian/Maldives",
                                            "Indian/Mauritius",
                                            "Indian/Mayotte",
                                            "Indian/Reunion",
                                            "Iran",
                                            "Israel",
                                            "Jamaica",
                                            "Japan",
                                            "Kwajalein",
                                            "Libya",
                                            "MET",
                                            "MST",
                                            "MST7MDT",
                                            "Mexico/BajaNorte",
                                            "Mexico/BajaSur",
                                            "Mexico/General",
                                            "NZ",
                                            "NZ-CHAT",
                                            "Navajo",
                                            "PRC",
                                            "PST8PDT",
                                            "Pacific/Apia",
                                            "Pacific/Auckland",
                                            "Pacific/Chatham",
                                            "Pacific/Chuuk",
                                            "Pacific/Easter",
                                            "Pacific/Efate",
                                            "Pacific/Enderbury",
                                            "Pacific/Fakaofo",
                                            "Pacific/Fiji",
                                            "Pacific/Funafuti",
                                            "Pacific/Galapagos",
                                            "Pacific/Gambier",
                                            "Pacific/Guadalcanal",
                                            "Pacific/Guam",
                                            "Pacific/Honolulu",
                                            "Pacific/Johnston",
                                            "Pacific/Kiritimati",
                                            "Pacific/Kosrae",
                                            "Pacific/Kwajalein",
                                            "Pacific/Majuro",
                                            "Pacific/Marquesas",
                                            "Pacific/Midway",
                                            "Pacific/Nauru",
                                            "Pacific/Niue",
                                            "Pacific/Norfolk",
                                            "Pacific/Noumea",
                                            "Pacific/Pago_Pago",
                                            "Pacific/Palau",
                                            "Pacific/Pitcairn",
                                            "Pacific/Pohnpei",
                                            "Pacific/Ponape",
                                            "Pacific/Port_Moresby",
                                            "Pacific/Rarotonga",
                                            "Pacific/Saipan",
                                            "Pacific/Samoa",
                                            "Pacific/Tahiti",
                                            "Pacific/Tarawa",
                                            "Pacific/Tongatapu",
                                            "Pacific/Truk",
                                            "Pacific/Wake",
                                            "Pacific/Wallis",
                                            "Pacific/Yap",
                                            "Poland",
                                            "Portugal",
                                            "ROC",
                                            "ROK",
                                            "Singapore",
                                            "Turkey",
                                            "UCT",
                                            "US/Alaska",
                                            "US/Aleutian",
                                            "US/Arizona",
                                            "US/Central",
                                            "US/East-Indiana",
                                            "US/Eastern",
                                            "US/Hawaii",
                                            "US/Indiana-Starke",
                                            "US/Michigan",
                                            "US/Mountain",
                                            "US/Pacific",
                                            "US/Pacific-New",
                                            "US/Samoa",
                                            "UTC",
                                            "Universal",
                                            "W-SU",
                                            "WET",
                                            "Zulu"};

    static {
        timezoneMap.put("Africa/Abidjan", 0);
        timezoneMap.put("Africa/Accra", 1);
        timezoneMap.put("Africa/Addis_Ababa", 2);
        timezoneMap.put("Africa/Algiers", 3);
        timezoneMap.put("Africa/Asmara", 4);
        timezoneMap.put("Africa/Asmera", 5);
        timezoneMap.put("Africa/Bamako", 6);
        timezoneMap.put("Africa/Bangui", 7);
        timezoneMap.put("Africa/Banjul", 8);
        timezoneMap.put("Africa/Bissau", 9);
        timezoneMap.put("Africa/Blantyre", 10);
        timezoneMap.put("Africa/Brazzaville", 11);
        timezoneMap.put("Africa/Bujumbura", 12);
        timezoneMap.put("Africa/Cairo", 13);
        timezoneMap.put("Africa/Casablanca", 14);
        timezoneMap.put("Africa/Ceuta", 15);
        timezoneMap.put("Africa/Conakry", 16);
        timezoneMap.put("Africa/Dakar", 17);
        timezoneMap.put("Africa/Dar_es_Salaam", 18);
        timezoneMap.put("Africa/Djibouti", 19);
        timezoneMap.put("Africa/Douala", 20);
        timezoneMap.put("Africa/El_Aaiun", 21);
        timezoneMap.put("Africa/Freetown", 22);
        timezoneMap.put("Africa/Gaborone", 23);
        timezoneMap.put("Africa/Harare", 24);
        timezoneMap.put("Africa/Johannesburg", 25);
        timezoneMap.put("Africa/Juba", 26);
        timezoneMap.put("Africa/Kampala", 27);
        timezoneMap.put("Africa/Khartoum", 28);
        timezoneMap.put("Africa/Kigali", 29);
        timezoneMap.put("Africa/Kinshasa", 30);
        timezoneMap.put("Africa/Lagos", 31);
        timezoneMap.put("Africa/Libreville", 32);
        timezoneMap.put("Africa/Lome", 33);
        timezoneMap.put("Africa/Luanda", 34);
        timezoneMap.put("Africa/Lubumbashi", 35);
        timezoneMap.put("Africa/Lusaka", 36);
        timezoneMap.put("Africa/Malabo", 37);
        timezoneMap.put("Africa/Maputo", 38);
        timezoneMap.put("Africa/Maseru", 39);
        timezoneMap.put("Africa/Mbabane", 40);
        timezoneMap.put("Africa/Mogadishu", 41);
        timezoneMap.put("Africa/Monrovia", 42);
        timezoneMap.put("Africa/Nairobi", 43);
        timezoneMap.put("Africa/Ndjamena", 44);
        timezoneMap.put("Africa/Niamey", 45);
        timezoneMap.put("Africa/Nouakchott", 46);
        timezoneMap.put("Africa/Ouagadougou", 47);
        timezoneMap.put("Africa/Porto-Novo", 48);
        timezoneMap.put("Africa/Sao_Tome", 49);
        timezoneMap.put("Africa/Timbuktu", 50);
        timezoneMap.put("Africa/Tripoli", 51);
        timezoneMap.put("Africa/Tunis", 52);
        timezoneMap.put("Africa/Windhoek", 53);
        timezoneMap.put("America/Adak", 54);
        timezoneMap.put("America/Anchorage", 55);
        timezoneMap.put("America/Anguilla", 56);
        timezoneMap.put("America/Antigua", 57);
        timezoneMap.put("America/Araguaina", 58);
        timezoneMap.put("America/Argentina/Buenos_Aires", 59);
        timezoneMap.put("America/Argentina/Catamarca", 60);
        timezoneMap.put("America/Argentina/ComodRivadavia", 61);
        timezoneMap.put("America/Argentina/Cordoba", 62);
        timezoneMap.put("America/Argentina/Jujuy", 63);
        timezoneMap.put("America/Argentina/La_Rioja", 64);
        timezoneMap.put("America/Argentina/Mendoza", 65);
        timezoneMap.put("America/Argentina/Rio_Gallegos", 66);
        timezoneMap.put("America/Argentina/Salta", 67);
        timezoneMap.put("America/Argentina/San_Juan", 68);
        timezoneMap.put("America/Argentina/San_Luis", 69);
        timezoneMap.put("America/Argentina/Tucuman", 70);
        timezoneMap.put("America/Argentina/Ushuaia", 71);
        timezoneMap.put("America/Aruba", 72);
        timezoneMap.put("America/Asuncion", 73);
        timezoneMap.put("America/Atikokan", 74);
        timezoneMap.put("America/Atka", 75);
        timezoneMap.put("America/Bahia", 76);
        timezoneMap.put("America/Bahia_Banderas", 77);
        timezoneMap.put("America/Barbados", 78);
        timezoneMap.put("America/Belem", 79);
        timezoneMap.put("America/Belize", 80);
        timezoneMap.put("America/Blanc-Sablon", 81);
        timezoneMap.put("America/Boa_Vista", 82);
        timezoneMap.put("America/Bogota", 83);
        timezoneMap.put("America/Boise", 84);
        timezoneMap.put("America/Buenos_Aires", 85);
        timezoneMap.put("America/Cambridge_Bay", 86);
        timezoneMap.put("America/Campo_Grande", 87);
        timezoneMap.put("America/Cancun", 88);
        timezoneMap.put("America/Caracas", 89);
        timezoneMap.put("America/Catamarca", 90);
        timezoneMap.put("America/Cayenne", 91);
        timezoneMap.put("America/Cayman", 92);
        timezoneMap.put("America/Chicago", 93);
        timezoneMap.put("America/Chihuahua", 94);
        timezoneMap.put("America/Coral_Harbour", 95);
        timezoneMap.put("America/Cordoba", 96);
        timezoneMap.put("America/Costa_Rica", 97);
        timezoneMap.put("America/Cuiaba", 98);
        timezoneMap.put("America/Curacao", 99);
        timezoneMap.put("America/Danmarkshavn", 100);
        timezoneMap.put("America/Dawson", 101);
        timezoneMap.put("America/Dawson_Creek", 102);
        timezoneMap.put("America/Denver", 103);
        timezoneMap.put("America/Detroit", 104);
        timezoneMap.put("America/Dominica", 105);
        timezoneMap.put("America/Edmonton", 106);
        timezoneMap.put("America/Eirunepe", 107);
        timezoneMap.put("America/El_Salvador", 108);
        timezoneMap.put("America/Ensenada", 109);
        timezoneMap.put("America/Fort_Wayne", 110);
        timezoneMap.put("America/Fortaleza", 111);
        timezoneMap.put("America/Glace_Bay", 112);
        timezoneMap.put("America/Godthab", 113);
        timezoneMap.put("America/Goose_Bay", 114);
        timezoneMap.put("America/Grand_Turk", 115);
        timezoneMap.put("America/Grenada", 116);
        timezoneMap.put("America/Guadeloupe", 117);
        timezoneMap.put("America/Guatemala", 118);
        timezoneMap.put("America/Guayaquil", 119);
        timezoneMap.put("America/Guyana", 120);
        timezoneMap.put("America/Halifax", 121);
        timezoneMap.put("America/Havana", 122);
        timezoneMap.put("America/Hermosillo", 123);
        timezoneMap.put("America/Indiana/Indianapolis", 124);
        timezoneMap.put("America/Indiana/Knox", 125);
        timezoneMap.put("America/Indiana/Marengo", 126);
        timezoneMap.put("America/Indiana/Petersburg", 127);
        timezoneMap.put("America/Indiana/Tell_City", 128);
        timezoneMap.put("America/Indiana/Vevay", 129);
        timezoneMap.put("America/Indiana/Vincennes", 130);
        timezoneMap.put("America/Indiana/Winamac", 131);
        timezoneMap.put("America/Indianapolis", 132);
        timezoneMap.put("America/Inuvik", 133);
        timezoneMap.put("America/Iqaluit", 134);
        timezoneMap.put("America/Jamaica", 135);
        timezoneMap.put("America/Jujuy", 136);
        timezoneMap.put("America/Juneau", 137);
        timezoneMap.put("America/Kentucky/Louisville", 138);
        timezoneMap.put("America/Kentucky/Monticello", 139);
        timezoneMap.put("America/Knox_IN", 140);
        timezoneMap.put("America/Kralendijk", 141);
        timezoneMap.put("America/La_Paz", 142);
        timezoneMap.put("America/Lima", 143);
        timezoneMap.put("America/Los_Angeles", 144);
        timezoneMap.put("America/Louisville", 145);
        timezoneMap.put("America/Lower_Princes", 146);
        timezoneMap.put("America/Maceio", 147);
        timezoneMap.put("America/Managua", 148);
        timezoneMap.put("America/Manaus", 149);
        timezoneMap.put("America/Marigot", 150);
        timezoneMap.put("America/Martinique", 151);
        timezoneMap.put("America/Matamoros", 152);
        timezoneMap.put("America/Mazatlan", 153);
        timezoneMap.put("America/Mendoza", 154);
        timezoneMap.put("America/Menominee", 155);
        timezoneMap.put("America/Merida", 156);
        timezoneMap.put("America/Metlakatla", 157);
        timezoneMap.put("America/Mexico_City", 158);
        timezoneMap.put("America/Miquelon", 159);
        timezoneMap.put("America/Moncton", 160);
        timezoneMap.put("America/Monterrey", 161);
        timezoneMap.put("America/Montevideo", 162);
        timezoneMap.put("America/Montreal", 163);
        timezoneMap.put("America/Montserrat", 164);
        timezoneMap.put("America/Nassau", 165);
        timezoneMap.put("America/New_York", 166);
        timezoneMap.put("America/Nipigon", 167);
        timezoneMap.put("America/Nome", 168);
        timezoneMap.put("America/Noronha", 169);
        timezoneMap.put("America/North_Dakota/Beulah", 170);
        timezoneMap.put("America/North_Dakota/Center", 171);
        timezoneMap.put("America/North_Dakota/New_Salem", 172);
        timezoneMap.put("America/Ojinaga", 173);
        timezoneMap.put("America/Panama", 174);
        timezoneMap.put("America/Pangnirtung", 175);
        timezoneMap.put("America/Paramaribo", 176);
        timezoneMap.put("America/Phoenix", 177);
        timezoneMap.put("America/Port-au-Prince", 178);
        timezoneMap.put("America/Port_of_Spain", 179);
        timezoneMap.put("America/Porto_Acre", 180);
        timezoneMap.put("America/Porto_Velho", 181);
        timezoneMap.put("America/Puerto_Rico", 182);
        timezoneMap.put("America/Rainy_River", 183);
        timezoneMap.put("America/Rankin_Inlet", 184);
        timezoneMap.put("America/Recife", 185);
        timezoneMap.put("America/Regina", 186);
        timezoneMap.put("America/Resolute", 187);
        timezoneMap.put("America/Rio_Branco", 188);
        timezoneMap.put("America/Rosario", 189);
        timezoneMap.put("America/Santa_Isabel", 190);
        timezoneMap.put("America/Santarem", 191);
        timezoneMap.put("America/Santiago", 192);
        timezoneMap.put("America/Santo_Domingo", 193);
        timezoneMap.put("America/Sao_Paulo", 194);
        timezoneMap.put("America/Scoresbysund", 195);
        timezoneMap.put("America/Shiprock", 196);
        timezoneMap.put("America/Sitka", 197);
        timezoneMap.put("America/St_Barthelemy", 198);
        timezoneMap.put("America/St_Johns", 199);
        timezoneMap.put("America/St_Kitts", 200);
        timezoneMap.put("America/St_Lucia", 201);
        timezoneMap.put("America/St_Thomas", 202);
        timezoneMap.put("America/St_Vincent", 203);
        timezoneMap.put("America/Swift_Current", 204);
        timezoneMap.put("America/Tegucigalpa", 205);
        timezoneMap.put("America/Thule", 206);
        timezoneMap.put("America/Thunder_Bay", 207);
        timezoneMap.put("America/Tijuana", 208);
        timezoneMap.put("America/Toronto", 209);
        timezoneMap.put("America/Tortola", 210);
        timezoneMap.put("America/Vancouver", 211);
        timezoneMap.put("America/Virgin", 212);
        timezoneMap.put("America/Whitehorse", 213);
        timezoneMap.put("America/Winnipeg", 214);
        timezoneMap.put("America/Yakutat", 215);
        timezoneMap.put("America/Yellowknife", 216);
        timezoneMap.put("Antarctica/Casey", 217);
        timezoneMap.put("Antarctica/Davis", 218);
        timezoneMap.put("Antarctica/DumontDUrville", 219);
        timezoneMap.put("Antarctica/Macquarie", 220);
        timezoneMap.put("Antarctica/Mawson", 221);
        timezoneMap.put("Antarctica/McMurdo", 222);
        timezoneMap.put("Antarctica/Palmer", 223);
        timezoneMap.put("Antarctica/Rothera", 224);
        timezoneMap.put("Antarctica/South_Pole", 225);
        timezoneMap.put("Antarctica/Syowa", 226);
        timezoneMap.put("Antarctica/Vostok", 227);
        timezoneMap.put("Arctic/Longyearbyen", 228);
        timezoneMap.put("Asia/Aden", 229);
        timezoneMap.put("Asia/Almaty", 230);
        timezoneMap.put("Asia/Amman", 231);
        timezoneMap.put("Asia/Anadyr", 232);
        timezoneMap.put("Asia/Aqtau", 233);
        timezoneMap.put("Asia/Aqtobe", 234);
        timezoneMap.put("Asia/Ashgabat", 235);
        timezoneMap.put("Asia/Ashkhabad", 236);
        timezoneMap.put("Asia/Baghdad", 237);
        timezoneMap.put("Asia/Bahrain", 238);
        timezoneMap.put("Asia/Baku", 239);
        timezoneMap.put("Asia/Bangkok", 240);
        timezoneMap.put("Asia/Beirut", 241);
        timezoneMap.put("Asia/Bishkek", 242);
        timezoneMap.put("Asia/Brunei", 243);
        timezoneMap.put("Asia/Calcutta", 244);
        timezoneMap.put("Asia/Choibalsan", 245);
        timezoneMap.put("Asia/Chongqing", 246);
        timezoneMap.put("Asia/Chungking", 247);
        timezoneMap.put("Asia/Colombo", 248);
        timezoneMap.put("Asia/Dacca", 249);
        timezoneMap.put("Asia/Damascus", 250);
        timezoneMap.put("Asia/Dhaka", 251);
        timezoneMap.put("Asia/Dili", 252);
        timezoneMap.put("Asia/Dubai", 253);
        timezoneMap.put("Asia/Dushanbe", 254);
        timezoneMap.put("Asia/Gaza", 255);
        timezoneMap.put("Asia/Harbin", 256);
        timezoneMap.put("Asia/Hebron", 257);
        timezoneMap.put("Asia/Ho_Chi_Minh", 258);
        timezoneMap.put("Asia/Hong_Kong", 259);
        timezoneMap.put("Asia/Hovd", 260);
        timezoneMap.put("Asia/Irkutsk", 261);
        timezoneMap.put("Asia/Istanbul", 262);
        timezoneMap.put("Asia/Jakarta", 263);
        timezoneMap.put("Asia/Jayapura", 264);
        timezoneMap.put("Asia/Jerusalem", 265);
        timezoneMap.put("Asia/Kabul", 266);
        timezoneMap.put("Asia/Kamchatka", 267);
        timezoneMap.put("Asia/Karachi", 268);
        timezoneMap.put("Asia/Kashgar", 269);
        timezoneMap.put("Asia/Kathmandu", 270);
        timezoneMap.put("Asia/Katmandu", 271);
        timezoneMap.put("Asia/Kolkata", 272);
        timezoneMap.put("Asia/Krasnoyarsk", 273);
        timezoneMap.put("Asia/Kuala_Lumpur", 274);
        timezoneMap.put("Asia/Kuching", 275);
        timezoneMap.put("Asia/Kuwait", 276);
        timezoneMap.put("Asia/Macao", 277);
        timezoneMap.put("Asia/Macau", 278);
        timezoneMap.put("Asia/Magadan", 279);
        timezoneMap.put("Asia/Makassar", 280);
        timezoneMap.put("Asia/Manila", 281);
        timezoneMap.put("Asia/Muscat", 282);
        timezoneMap.put("Asia/Nicosia", 283);
        timezoneMap.put("Asia/Novokuznetsk", 284);
        timezoneMap.put("Asia/Novosibirsk", 285);
        timezoneMap.put("Asia/Omsk", 286);
        timezoneMap.put("Asia/Oral", 287);
        timezoneMap.put("Asia/Phnom_Penh", 288);
        timezoneMap.put("Asia/Pontianak", 289);
        timezoneMap.put("Asia/Pyongyang", 290);
        timezoneMap.put("Asia/Qatar", 291);
        timezoneMap.put("Asia/Qyzylorda", 292);
        timezoneMap.put("Asia/Rangoon", 293);
        timezoneMap.put("Asia/Riyadh", 294);
        timezoneMap.put("Asia/Saigon", 295);
        timezoneMap.put("Asia/Sakhalin", 296);
        timezoneMap.put("Asia/Samarkand", 297);
        timezoneMap.put("Asia/Seoul", 298);
        timezoneMap.put("Asia/Shanghai", 299);
        timezoneMap.put("Asia/Singapore", 300);
        timezoneMap.put("Asia/Taipei", 301);
        timezoneMap.put("Asia/Tashkent", 302);
        timezoneMap.put("Asia/Tbilisi", 303);
        timezoneMap.put("Asia/Tehran", 304);
        timezoneMap.put("Asia/Tel_Aviv", 305);
        timezoneMap.put("Asia/Thimbu", 306);
        timezoneMap.put("Asia/Thimphu", 307);
        timezoneMap.put("Asia/Tokyo", 308);
        timezoneMap.put("Asia/Ujung_Pandang", 309);
        timezoneMap.put("Asia/Ulaanbaatar", 310);
        timezoneMap.put("Asia/Ulan_Bator", 311);
        timezoneMap.put("Asia/Urumqi", 312);
        timezoneMap.put("Asia/Vientiane", 313);
        timezoneMap.put("Asia/Vladivostok", 314);
        timezoneMap.put("Asia/Yakutsk", 315);
        timezoneMap.put("Asia/Yekaterinburg", 316);
        timezoneMap.put("Asia/Yerevan", 317);
        timezoneMap.put("Atlantic/Azores", 318);
        timezoneMap.put("Atlantic/Bermuda", 319);
        timezoneMap.put("Atlantic/Canary", 320);
        timezoneMap.put("Atlantic/Cape_Verde", 321);
        timezoneMap.put("Atlantic/Faeroe", 322);
        timezoneMap.put("Atlantic/Faroe", 323);
        timezoneMap.put("Atlantic/Jan_Mayen", 324);
        timezoneMap.put("Atlantic/Madeira", 325);
        timezoneMap.put("Atlantic/Reykjavik", 326);
        timezoneMap.put("Atlantic/South_Georgia", 327);
        timezoneMap.put("Atlantic/St_Helena", 328);
        timezoneMap.put("Atlantic/Stanley", 329);
        timezoneMap.put("Australia/ACT", 330);
        timezoneMap.put("Australia/Adelaide", 331);
        timezoneMap.put("Australia/Brisbane", 332);
        timezoneMap.put("Australia/Broken_Hill", 333);
        timezoneMap.put("Australia/Canberra", 334);
        timezoneMap.put("Australia/Currie", 335);
        timezoneMap.put("Australia/Darwin", 336);
        timezoneMap.put("Australia/Eucla", 337);
        timezoneMap.put("Australia/Hobart", 338);
        timezoneMap.put("Australia/LHI", 339);
        timezoneMap.put("Australia/Lindeman", 340);
        timezoneMap.put("Australia/Lord_Howe", 341);
        timezoneMap.put("Australia/Melbourne", 342);
        timezoneMap.put("Australia/NSW", 343);
        timezoneMap.put("Australia/North", 344);
        timezoneMap.put("Australia/Perth", 345);
        timezoneMap.put("Australia/Queensland", 346);
        timezoneMap.put("Australia/South", 347);
        timezoneMap.put("Australia/Sydney", 348);
        timezoneMap.put("Australia/Tasmania", 349);
        timezoneMap.put("Australia/Victoria", 350);
        timezoneMap.put("Australia/West", 351);
        timezoneMap.put("Australia/Yancowinna", 352);
        timezoneMap.put("Brazil/Acre", 353);
        timezoneMap.put("Brazil/DeNoronha", 354);
        timezoneMap.put("Brazil/East", 355);
        timezoneMap.put("Brazil/West", 356);
        timezoneMap.put("CET", 357);
        timezoneMap.put("CST6CDT", 358);
        timezoneMap.put("Canada/Atlantic", 359);
        timezoneMap.put("Canada/Central", 360);
        timezoneMap.put("Canada/East-Saskatchewan", 361);
        timezoneMap.put("Canada/Eastern", 362);
        timezoneMap.put("Canada/Mountain", 363);
        timezoneMap.put("Canada/Newfoundland", 364);
        timezoneMap.put("Canada/Pacific", 365);
        timezoneMap.put("Canada/Saskatchewan", 366);
        timezoneMap.put("Canada/Yukon", 367);
        timezoneMap.put("Chile/Continental", 368);
        timezoneMap.put("Chile/EasterIsland", 369);
        timezoneMap.put("Cuba", 370);
        timezoneMap.put("EET", 371);
        timezoneMap.put("EST", 372);
        timezoneMap.put("EST5EDT", 373);
        timezoneMap.put("Egypt", 374);
        timezoneMap.put("Eire", 375);
        timezoneMap.put("Etc/GMT", 376);
        timezoneMap.put("Etc/GMT+0", 377);
        timezoneMap.put("Etc/GMT+1", 378);
        timezoneMap.put("Etc/GMT+10", 379);
        timezoneMap.put("Etc/GMT+11", 380);
        timezoneMap.put("Etc/GMT+12", 381);
        timezoneMap.put("Etc/GMT+2", 382);
        timezoneMap.put("Etc/GMT+3", 383);
        timezoneMap.put("Etc/GMT+4", 384);
        timezoneMap.put("Etc/GMT+5", 385);
        timezoneMap.put("Etc/GMT+6", 386);
        timezoneMap.put("Etc/GMT+7", 387);
        timezoneMap.put("Etc/GMT+8", 388);
        timezoneMap.put("Etc/GMT+9", 389);
        timezoneMap.put("Etc/GMT-0", 390);
        timezoneMap.put("Etc/GMT-1", 391);
        timezoneMap.put("Etc/GMT-10", 392);
        timezoneMap.put("Etc/GMT-11", 393);
        timezoneMap.put("Etc/GMT-12", 394);
        timezoneMap.put("Etc/GMT-13", 395);
        timezoneMap.put("Etc/GMT-14", 396);
        timezoneMap.put("Etc/GMT-2", 397);
        timezoneMap.put("Etc/GMT-3", 398);
        timezoneMap.put("Etc/GMT-4", 399);
        timezoneMap.put("Etc/GMT-5", 400);
        timezoneMap.put("Etc/GMT-6", 401);
        timezoneMap.put("Etc/GMT-7", 402);
        timezoneMap.put("Etc/GMT-8", 403);
        timezoneMap.put("Etc/GMT-9", 404);
        timezoneMap.put("Etc/GMT0", 405);
        timezoneMap.put("Etc/Greenwich", 406);
        timezoneMap.put("Etc/UCT", 407);
        timezoneMap.put("Etc/UTC", 408);
        timezoneMap.put("Etc/Universal", 409);
        timezoneMap.put("Etc/Zulu", 410);
        timezoneMap.put("Europe/Amsterdam", 411);
        timezoneMap.put("Europe/Andorra", 412);
        timezoneMap.put("Europe/Athens", 413);
        timezoneMap.put("Europe/Belfast", 414);
        timezoneMap.put("Europe/Belgrade", 415);
        timezoneMap.put("Europe/Berlin", 416);
        timezoneMap.put("Europe/Bratislava", 417);
        timezoneMap.put("Europe/Brussels", 418);
        timezoneMap.put("Europe/Bucharest", 419);
        timezoneMap.put("Europe/Budapest", 420);
        timezoneMap.put("Europe/Chisinau", 421);
        timezoneMap.put("Europe/Copenhagen", 422);
        timezoneMap.put("Europe/Dublin", 423);
        timezoneMap.put("Europe/Gibraltar", 424);
        timezoneMap.put("Europe/Guernsey", 425);
        timezoneMap.put("Europe/Helsinki", 426);
        timezoneMap.put("Europe/Isle_of_Man", 427);
        timezoneMap.put("Europe/Istanbul", 428);
        timezoneMap.put("Europe/Jersey", 429);
        timezoneMap.put("Europe/Kaliningrad", 430);
        timezoneMap.put("Europe/Kiev", 431);
        timezoneMap.put("Europe/Lisbon", 432);
        timezoneMap.put("Europe/Ljubljana", 433);
        timezoneMap.put("Europe/London", 434);
        timezoneMap.put("Europe/Luxembourg", 435);
        timezoneMap.put("Europe/Madrid", 436);
        timezoneMap.put("Europe/Malta", 437);
        timezoneMap.put("Europe/Mariehamn", 438);
        timezoneMap.put("Europe/Minsk", 439);
        timezoneMap.put("Europe/Monaco", 440);
        timezoneMap.put("Europe/Moscow", 441);
        timezoneMap.put("Europe/Nicosia", 442);
        timezoneMap.put("Europe/Oslo", 443);
        timezoneMap.put("Europe/Paris", 444);
        timezoneMap.put("Europe/Podgorica", 445);
        timezoneMap.put("Europe/Prague", 446);
        timezoneMap.put("Europe/Riga", 447);
        timezoneMap.put("Europe/Rome", 448);
        timezoneMap.put("Europe/Samara", 449);
        timezoneMap.put("Europe/San_Marino", 450);
        timezoneMap.put("Europe/Sarajevo", 451);
        timezoneMap.put("Europe/Simferopol", 452);
        timezoneMap.put("Europe/Skopje", 453);
        timezoneMap.put("Europe/Sofia", 454);
        timezoneMap.put("Europe/Stockholm", 455);
        timezoneMap.put("Europe/Tallinn", 456);
        timezoneMap.put("Europe/Tirane", 457);
        timezoneMap.put("Europe/Tiraspol", 458);
        timezoneMap.put("Europe/Uzhgorod", 459);
        timezoneMap.put("Europe/Vaduz", 460);
        timezoneMap.put("Europe/Vatican", 461);
        timezoneMap.put("Europe/Vienna", 462);
        timezoneMap.put("Europe/Vilnius", 463);
        timezoneMap.put("Europe/Volgograd", 464);
        timezoneMap.put("Europe/Warsaw", 465);
        timezoneMap.put("Europe/Zagreb", 466);
        timezoneMap.put("Europe/Zaporozhye", 467);
        timezoneMap.put("Europe/Zurich", 468);
        timezoneMap.put("GB", 469);
        timezoneMap.put("GB-Eire", 470);
        timezoneMap.put("GMT", 471);
        timezoneMap.put("GMT+0", 472);
        timezoneMap.put("GMT-0", 473);
        timezoneMap.put("GMT0", 474);
        timezoneMap.put("Greenwich", 475);
        timezoneMap.put("HST", 476);
        timezoneMap.put("Hongkong", 477);
        timezoneMap.put("Iceland", 478);
        timezoneMap.put("Indian/Antananarivo", 479);
        timezoneMap.put("Indian/Chagos", 480);
        timezoneMap.put("Indian/Christmas", 481);
        timezoneMap.put("Indian/Cocos", 482);
        timezoneMap.put("Indian/Comoro", 483);
        timezoneMap.put("Indian/Kerguelen", 484);
        timezoneMap.put("Indian/Mahe", 485);
        timezoneMap.put("Indian/Maldives", 486);
        timezoneMap.put("Indian/Mauritius", 487);
        timezoneMap.put("Indian/Mayotte", 488);
        timezoneMap.put("Indian/Reunion", 489);
        timezoneMap.put("Iran", 490);
        timezoneMap.put("Israel", 491);
        timezoneMap.put("Jamaica", 492);
        timezoneMap.put("Japan", 493);
        timezoneMap.put("Kwajalein", 494);
        timezoneMap.put("Libya", 495);
        timezoneMap.put("MET", 496);
        timezoneMap.put("MST", 497);
        timezoneMap.put("MST7MDT", 498);
        timezoneMap.put("Mexico/BajaNorte", 499);
        timezoneMap.put("Mexico/BajaSur", 500);
        timezoneMap.put("Mexico/General", 501);
        timezoneMap.put("NZ", 502);
        timezoneMap.put("NZ-CHAT", 503);
        timezoneMap.put("Navajo", 504);
        timezoneMap.put("PRC", 505);
        timezoneMap.put("PST8PDT", 506);
        timezoneMap.put("Pacific/Apia", 507);
        timezoneMap.put("Pacific/Auckland", 508);
        timezoneMap.put("Pacific/Chatham", 509);
        timezoneMap.put("Pacific/Chuuk", 510);
        timezoneMap.put("Pacific/Easter", 511);
        timezoneMap.put("Pacific/Efate", 512);
        timezoneMap.put("Pacific/Enderbury", 513);
        timezoneMap.put("Pacific/Fakaofo", 514);
        timezoneMap.put("Pacific/Fiji", 515);
        timezoneMap.put("Pacific/Funafuti", 516);
        timezoneMap.put("Pacific/Galapagos", 517);
        timezoneMap.put("Pacific/Gambier", 518);
        timezoneMap.put("Pacific/Guadalcanal", 519);
        timezoneMap.put("Pacific/Guam", 520);
        timezoneMap.put("Pacific/Honolulu", 521);
        timezoneMap.put("Pacific/Johnston", 522);
        timezoneMap.put("Pacific/Kiritimati", 523);
        timezoneMap.put("Pacific/Kosrae", 524);
        timezoneMap.put("Pacific/Kwajalein", 525);
        timezoneMap.put("Pacific/Majuro", 526);
        timezoneMap.put("Pacific/Marquesas", 527);
        timezoneMap.put("Pacific/Midway", 528);
        timezoneMap.put("Pacific/Nauru", 529);
        timezoneMap.put("Pacific/Niue", 530);
        timezoneMap.put("Pacific/Norfolk", 531);
        timezoneMap.put("Pacific/Noumea", 532);
        timezoneMap.put("Pacific/Pago_Pago", 533);
        timezoneMap.put("Pacific/Palau", 534);
        timezoneMap.put("Pacific/Pitcairn", 535);
        timezoneMap.put("Pacific/Pohnpei", 536);
        timezoneMap.put("Pacific/Ponape", 537);
        timezoneMap.put("Pacific/Port_Moresby", 538);
        timezoneMap.put("Pacific/Rarotonga", 539);
        timezoneMap.put("Pacific/Saipan", 540);
        timezoneMap.put("Pacific/Samoa", 541);
        timezoneMap.put("Pacific/Tahiti", 542);
        timezoneMap.put("Pacific/Tarawa", 543);
        timezoneMap.put("Pacific/Tongatapu", 544);
        timezoneMap.put("Pacific/Truk", 545);
        timezoneMap.put("Pacific/Wake", 546);
        timezoneMap.put("Pacific/Wallis", 547);
        timezoneMap.put("Pacific/Yap", 548);
        timezoneMap.put("Poland", 549);
        timezoneMap.put("Portugal", 550);
        timezoneMap.put("ROC", 551);
        timezoneMap.put("ROK", 552);
        timezoneMap.put("Singapore", 553);
        timezoneMap.put("Turkey", 554);
        timezoneMap.put("UCT", 555);
        timezoneMap.put("US/Alaska", 556);
        timezoneMap.put("US/Aleutian", 557);
        timezoneMap.put("US/Arizona", 558);
        timezoneMap.put("US/Central", 559);
        timezoneMap.put("US/East-Indiana", 560);
        timezoneMap.put("US/Eastern", 561);
        timezoneMap.put("US/Hawaii", 562);
        timezoneMap.put("US/Indiana-Starke", 563);
        timezoneMap.put("US/Michigan", 564);
        timezoneMap.put("US/Mountain", 565);
        timezoneMap.put("US/Pacific", 566);
        timezoneMap.put("US/Pacific-New", 567);
        timezoneMap.put("US/Samoa", 568);
        timezoneMap.put("UTC", 569);
        timezoneMap.put("Universal", 570);
        timezoneMap.put("W-SU", 571);
        timezoneMap.put("WET", 572);
        timezoneMap.put("Zulu", 573);
    }

    public static final DateTimeFormatter formatDate        = DateTimeFormat.forPattern("yyyy-MM-dd");
    public static final DateTimeFormatter formatTimeStamp    = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    public static final DateTimeFormatter formatTimeStampTZ = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS ZZZ");
    public static final DateTimeFormatter formatTime        = DateTimeFormat.forPattern("HH:mm:ss.SSS");

    public static DateTimeFormatter dateTimeTZFormat = null;
    public static DateTimeFormatter timeFormat = null;

    public static final int yearsToMonths = 12;
    public static final int hoursToMillis = 60 * 60 * 1000;
    public static final int minutesToMillis = 60 * 1000;
    public static final int secondsToMillis = 1000;

    public static int getIndex(String timezone) {
        return timezoneMap.get(timezone);
    }

    public static String getTimeZone(int index) {
        return timezoneList[index];
    }

    // Function returns the date time formatter used to parse date strings
    public static DateTimeFormatter getDateTimeFormatter() {

        if (dateTimeTZFormat == null) {
            DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
            DateTimeParser optionalTime = DateTimeFormat.forPattern(" HH:mm:ss").getParser();
            DateTimeParser optionalSec = DateTimeFormat.forPattern(".SSS").getParser();
            DateTimeParser optionalZone = DateTimeFormat.forPattern(" ZZZ").getParser();

            dateTimeTZFormat = new DateTimeFormatterBuilder().append(dateFormatter).appendOptional(optionalTime).appendOptional(optionalSec).appendOptional(optionalZone).toFormatter();
        }

        return dateTimeTZFormat;
    }

    // Function returns time formatter used to parse time strings
    public static DateTimeFormatter getTimeFormatter() {
        if (timeFormat == null) {
            DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("HH:mm:ss");
            DateTimeParser optionalSec = DateTimeFormat.forPattern(".SSS").getParser();
            timeFormat = new DateTimeFormatterBuilder().append(timeFormatter).appendOptional(optionalSec).toFormatter();
        }
        return timeFormat;
    }
}
