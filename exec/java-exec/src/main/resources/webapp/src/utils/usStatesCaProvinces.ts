/*
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

// Maps US state codes and names
export const US_STATES: Record<string, string> = {
  AL: 'Alabama', alabama: 'Alabama',
  AK: 'Alaska', alaska: 'Alaska',
  AZ: 'Arizona', arizona: 'Arizona',
  AR: 'Arkansas', arkansas: 'Arkansas',
  CA: 'California', california: 'California',
  CO: 'Colorado', colorado: 'Colorado',
  CT: 'Connecticut', connecticut: 'Connecticut',
  DE: 'Delaware', delaware: 'Delaware',
  FL: 'Florida', florida: 'Florida',
  GA: 'Georgia', georgia: 'Georgia',
  HI: 'Hawaii', hawaii: 'Hawaii',
  ID: 'Idaho', idaho: 'Idaho',
  IL: 'Illinois', illinois: 'Illinois',
  IN: 'Indiana', indiana: 'Indiana',
  IA: 'Iowa', iowa: 'Iowa',
  KS: 'Kansas', kansas: 'Kansas',
  KY: 'Kentucky', kentucky: 'Kentucky',
  LA: 'Louisiana', louisiana: 'Louisiana',
  ME: 'Maine', maine: 'Maine',
  MD: 'Maryland', maryland: 'Maryland',
  MA: 'Massachusetts', massachusetts: 'Massachusetts',
  MI: 'Michigan', michigan: 'Michigan',
  MN: 'Minnesota', minnesota: 'Minnesota',
  MS: 'Mississippi', mississippi: 'Mississippi',
  MO: 'Missouri', missouri: 'Missouri',
  MT: 'Montana', montana: 'Montana',
  NE: 'Nebraska', nebraska: 'Nebraska',
  NV: 'Nevada', nevada: 'Nevada',
  NH: 'New Hampshire', 'new hampshire': 'New Hampshire',
  NJ: 'New Jersey', 'new jersey': 'New Jersey',
  NM: 'New Mexico', 'new mexico': 'New Mexico',
  NY: 'New York', 'new york': 'New York',
  NC: 'North Carolina', 'north carolina': 'North Carolina',
  ND: 'North Dakota', 'north dakota': 'North Dakota',
  OH: 'Ohio', ohio: 'Ohio',
  OK: 'Oklahoma', oklahoma: 'Oklahoma',
  OR: 'Oregon', oregon: 'Oregon',
  PA: 'Pennsylvania', pennsylvania: 'Pennsylvania',
  RI: 'Rhode Island', 'rhode island': 'Rhode Island',
  SC: 'South Carolina', 'south carolina': 'South Carolina',
  SD: 'South Dakota', 'south dakota': 'South Dakota',
  TN: 'Tennessee', tennessee: 'Tennessee',
  TX: 'Texas', texas: 'Texas',
  UT: 'Utah', utah: 'Utah',
  VT: 'Vermont', vermont: 'Vermont',
  VA: 'Virginia', virginia: 'Virginia',
  WA: 'Washington', washington: 'Washington',
  WV: 'West Virginia', 'west virginia': 'West Virginia',
  WI: 'Wisconsin', wisconsin: 'Wisconsin',
  WY: 'Wyoming', wyoming: 'Wyoming',
};

// Maps Canadian province codes and names
export const CA_PROVINCES: Record<string, string> = {
  AB: 'Alberta', alberta: 'Alberta',
  BC: 'British Columbia', 'british columbia': 'British Columbia',
  MB: 'Manitoba', manitoba: 'Manitoba',
  NB: 'New Brunswick', 'new brunswick': 'New Brunswick',
  NL: 'Newfoundland and Labrador', 'newfoundland and labrador': 'Newfoundland and Labrador',
  NT: 'Northwest Territories', 'northwest territories': 'Northwest Territories',
  NS: 'Nova Scotia', 'nova scotia': 'Nova Scotia',
  NU: 'Nunavut', nunavut: 'Nunavut',
  ON: 'Ontario', ontario: 'Ontario',
  PE: 'Prince Edward Island', 'prince edward island': 'Prince Edward Island',
  QC: 'Quebec', quebec: 'Quebec',
  SK: 'Saskatchewan', saskatchewan: 'Saskatchewan',
  YT: 'Yukon', yukon: 'Yukon',
};

/**
 * Resolves a US state code (2-letter) or name to the canonical name.
 * Case-insensitive. Full names pass through.
 *
 * @param code - State code (e.g., "CA", "NY") or name (e.g., "California", "California")
 * @returns The canonical state name, or the original value if not found
 */
export function resolveUsStateName(code: string): string {
  const normalized = code.trim().toUpperCase();
  const lowerNorm = code.trim().toLowerCase().replace(/ +/g, ' ');
  return US_STATES[normalized] ?? US_STATES[lowerNorm] ?? code;
}

/**
 * Resolves a Canadian province code (2-letter) or name to the canonical name.
 * Case-insensitive. Full names pass through.
 *
 * @param code - Province code (e.g., "AB", "ON") or name (e.g., "Alberta", "Ontario")
 * @returns The canonical province name, or the original value if not found
 */
export function resolveCanadianProvinceName(code: string): string {
  const normalized = code.trim().toUpperCase();
  const lowerNorm = code.trim().toLowerCase().replace(/ +/g, ' ');
  return CA_PROVINCES[normalized] ?? CA_PROVINCES[lowerNorm] ?? code;
}

// Maps Mexican state names and codes
export const MX_STATES: Record<string, string> = {
  AG: 'Aguascalientes', aguascalientes: 'Aguascalientes',
  BC: 'Baja California', 'baja california': 'Baja California',
  BS: 'Baja California Sur', 'baja california sur': 'Baja California Sur',
  CM: 'Campeche', campeche: 'Campeche',
  CX: 'Ciudad de México', 'ciudad de méxico': 'Ciudad de México', 'cdmx': 'Ciudad de México',
  CO: 'Coahuila', coahuila: 'Coahuila',
  CL: 'Colima', colima: 'Colima',
  DF: 'Ciudad de México', 'distrito federal': 'Ciudad de México',
  DG: 'Durango', durango: 'Durango',
  GT: 'Guanajuato', guanajuato: 'Guanajuato',
  GR: 'Guerrero', guerrero: 'Guerrero',
  HG: 'Hidalgo', hidalgo: 'Hidalgo',
  JC: 'Jalisco', jalisco: 'Jalisco',
  MX: 'Estado de México', 'estado de méxico': 'Estado de México',
  MI: 'Michoacán', michoacán: 'Michoacán',
  MO: 'Morelos', morelos: 'Morelos',
  NA: 'Nayarit', nayarit: 'Nayarit',
  NL: 'Nuevo León', 'nuevo león': 'Nuevo León',
  OC: 'Oaxaca', oaxaca: 'Oaxaca',
  PB: 'Puebla', puebla: 'Puebla',
  QT: 'Querétaro', querétaro: 'Querétaro',
  QR: 'Quintana Roo', 'quintana roo': 'Quintana Roo',
  SL: 'San Luis Potosí', 'san luis potosí': 'San Luis Potosí',
  SI: 'Sinaloa', sinaloa: 'Sinaloa',
  SO: 'Sonora', sonora: 'Sonora',
  TB: 'Tabasco', tabasco: 'Tabasco',
  TM: 'Tamaulipas', tamaulipas: 'Tamaulipas',
  TL: 'Tlaxcala', tlaxcala: 'Tlaxcala',
  VZ: 'Veracruz', veracruz: 'Veracruz',
  YU: 'Yucatán', yucatán: 'Yucatán',
  ZA: 'Zacatecas', zacatecas: 'Zacatecas',
};

/**
 * Resolves a Mexican state code or name to the canonical name.
 * Case-insensitive.
 *
 * @param code - State code (e.g., "CDMX") or name
 * @returns The canonical state name, or the original value if not found
 */
export function resolveMexicanStateName(code: string): string {
  const normalized = code.trim().toUpperCase();
  const lowerNorm = code.trim().toLowerCase().replace(/ +/g, ' ');
  return MX_STATES[normalized] ?? MX_STATES[lowerNorm] ?? code;
}

// Maps Australian state and territory names
export const AU_STATES: Record<string, string> = {
  NSW: 'New South Wales',
  'new south wales': 'New South Wales',
  VIC: 'Victoria',
  victoria: 'Victoria',
  QLD: 'Queensland',
  queensland: 'Queensland',
  WA: 'Western Australia',
  'western australia': 'Western Australia',
  SA: 'South Australia',
  'south australia': 'South Australia',
  TAS: 'Tasmania',
  tasmania: 'Tasmania',
  ACT: 'Australian Capital Territory',
  'australian capital territory': 'Australian Capital Territory',
  NT: 'Northern Territory',
  'northern territory': 'Northern Territory',
};

/**
 * Resolves an Australian state/territory code or name to the canonical name.
 * Case-insensitive.
 */
export function resolveAustralianStateName(code: string): string {
  const normalized = code.trim().toUpperCase();
  const lowerNorm = code.trim().toLowerCase().replace(/ +/g, ' ');
  return AU_STATES[normalized] ?? AU_STATES[lowerNorm] ?? code;
}
