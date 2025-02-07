import random
import unidecode

def get_cities():
    cities_list = [
        "valencia", "torrent", "paiporta", "aldaia", "alfafar", "benetusser", "catarroja", "chiva", "massanassa", "albal",
        "silla", "alcasser", "picassent", "albalat_de_la_ribera", "alborache", "alcudia", "alginet", "bunyol",
        "catadau", "cheste", "godelleta", "guadassuar", "loriguilla", "almussafes", "alzira", "benifaio", "beniparrell",
        "betera", "bugarra", "calles", "camporrobles", "carlet", "corbera", "quart_de_poblet", "cullera", "chera",
        "dos_aguas", "xirivella", "albal", "benetusser", "catarroja", "chiva", "massanassa", "paiporta"
    ]

    return cities_list


def get_messages_affected():
    messages = [
    ("suministros", "Se necesita comida y agua potable para familias afectadas por la inundacion.", "comida_bebida"),
    ("suministros", "Se requiere ropa seca para personas que lo han perdido todo en la tormenta.", "ropa"),
    ("suministros", "Un refugio con 50 personas necesita productos de limpieza para desinfectar el area.", "productos_limpieza"),
    ("suministros", "Solicitamos alimentos no perecederos para un albergue temporal.", "comida_bebida"),
    ("suministros", "Familias desplazadas necesitan articulos de higiene personal.", "productos_limpieza"),

    ("limpieza_calles/casas", "Varias calles estan llenas de lodo y escombros, se necesitan palas para retirarlo.", "palas"),
    ("limpieza_calles/casas", "Un colegio ha sido inundado y necesita escobas y cubos para su limpieza.", "escobas"),
    ("limpieza_calles/casas", "Se buscan voluntarios con palas para ayudar en la limpieza de viviendas afectadas.", "palas"),
    ("limpieza_calles/casas", "Varias casas necesitan limpieza urgente para evitar infecciones, se requieren escobas.", "escobas"),
    ("limpieza_calles/casas", "Un centro comunitario necesita cubos y palas para retirar barro y residuos.", "cubos"),

    ("maquinaria", "Se necesita un tractor para remover escombros en una calle bloqueada.", "tractor"),
    ("maquinaria", "Buscamos una grua para levantar un vehiculo atrapado por el agua.", "grua"),
    ("maquinaria", "Se requiere un vehiculo 4x4 para acceder a zonas anegadas.", "4X4"),
    ("maquinaria", "Un barrio afectado necesita bombas de agua para extraer el agua estancada.", "bomba_agua"),
    ("maquinaria", "Un puente ha quedado bloqueado por arboles caidos, se requiere un tractor.", "tractor"),

    ("cuidados_medicos", "Se necesita atencion medica para personas con heridas leves.", "atencion_enfermos"),
    ("cuidados_medicos", "Un anciano con diabetes ha perdido su insulina y necesita medicacion.", "medicinas_diabetes"),
    ("cuidados_medicos", "Un paciente con problemas del corazon requiere su medicacion urgente.", "medicinas_corazon"),
    ("cuidados_medicos", "Un refugio necesita medicinas genericas para tratar gripes y fiebre.", "medicinas_genericas"),
    ("cuidados_medicos", "Varias personas en un albergue presentan sintomas de infeccion, se requiere atencion medica.", "atencion_enfermos"),

    ("transporte", "Se necesita transporte para evacuar a personas atrapadas en sus casas.", "transporte"),
    ("transporte", "Un grupo de ancianos necesita ser trasladado a un refugio seguro.", "transporte"),
    ("transporte", "Se requieren vehiculos para llevar suministros a zonas aisladas.", "transporte"),
    ("transporte", "Varias personas enfermas necesitan transporte para llegar a un centro de salud.", "transporte"),
    ("transporte", "Se busca un autobus para trasladar a familias afectadas por la inundacion.", "transporte"),

    ("suministros", "Se necesita agua potable para abastecer a un refugio con familias afectadas.", "comida_bebida"),
    ("suministros", "Un grupo de voluntarios busca alimentos no perecederos para repartir en barrios aislados.", "comida_bebida"),
    ("suministros", "Varias familias han perdido su ropa y necesitan abrigos y calzado seco.", "ropa"),
    ("suministros", "Un refugio temporal requiere productos de higiene personal y desinfectantes.", "productos_limpieza"),
    ("suministros", "Se solicita leche en polvo y pañales para bebes en un centro de acogida.", "comida_bebida"),

    ("limpieza_calles/casas", "Las calles de un barrio estan cubiertas de lodo y se requieren palas para la limpieza.", "palas"),
    ("limpieza_calles/casas", "Un centro comunitario necesita escobas para retirar el barro acumulado en el suelo.", "escobas"),
    ("limpieza_calles/casas", "Vecinos buscan cubos y palas para ayudar a limpiar casas inundadas.", "cubos"),
    ("limpieza_calles/casas", "Se necesitan voluntarios con escobas y productos de limpieza para desinfectar una escuela afectada.", "escobas"),
    ("limpieza_calles/casas", "Un albergue temporal requiere cubos para drenar el agua acumulada en el patio.", "cubos"),

    ("maquinaria", "Un tractor es necesario para remover escombros y despejar un camino bloqueado.", "tractor"),
    ("maquinaria", "Se necesita una grua para levantar un vehiculo atrapado en una zona anegada.", "grua"),
    ("maquinaria", "Un todoterreno 4x4 es requerido para llevar ayuda a un area de dificil acceso.", "4X4"),
    ("maquinaria", "Varias casas siguen inundadas y se requiere una bomba de agua para drenarlas.", "bomba_agua"),
    ("maquinaria", "Una excavadora es necesaria para retirar lodo acumulado en una carretera principal.", "tractor"),

    ("cuidados_medicos", "Un anciano con diabetes ha perdido su insulina y necesita reposicion urgente.", "medicinas_diabetes"),
    ("cuidados_medicos", "Un paciente con problemas cardiacos requiere atencion medica y medicamentos.", "medicinas_corazon"),
    ("cuidados_medicos", "Varias personas con heridas leves necesitan curaciones y primeros auxilios.", "atencion_enfermos"),
    ("cuidados_medicos", "Un refugio con muchas personas necesita medicamentos genericos y analgesicos.", "medicinas_genericas"),
    ("cuidados_medicos", "Una persona con fiebre alta requiere ser atendida por un medico lo antes posible.", "atencion_enfermos"),

    ("transporte", "Un grupo de personas necesita transporte para evacuar un barrio inundado.", "transporte"),
    ("transporte", "Ancianos sin movilidad requieren un vehiculo para trasladarse a un refugio seguro.", "transporte"),
    ("transporte", "Se necesita transporte para llevar alimentos y agua a zonas de dificil acceso.", "transporte"),
    ("transporte", "Varias familias buscan ayuda para trasladarse a casa de familiares fuera de la zona afectada.", "transporte"),
    ("transporte", "Un equipo de rescatistas necesita vehiculos para llegar a un area aislada por el agua.", "transporte")
]
    
    return messages


def generate_phone_number():
    return f'+34-{random.randint(600000000, 699999999)}'


def get_messages_volunteers():
    messages= [
    ("suministros", "Somos un grupo de voluntarios y llevamos comida y agua potable para los afectados.", "comida_bebida"),
    ("suministros", "Recolectamos ropa seca para distribuirla entre quienes la necesiten.", "ropa"),
    ("suministros", "Llevamos kits de higiene y productos de limpieza a los refugios.", "productos_limpieza"),
    ("suministros", "Tenemos alimentos no perecederos para repartir en zonas afectadas.", "comida_bebida"),
    ("suministros", "Estamos recolectando productos de higiene personal para entregar en los albergues.", "productos_limpieza"),

    ("limpieza_calles/casas", "Vamos con palas para ayudar a retirar el lodo de las calles.", "palas"),
    ("limpieza_calles/casas", "Llevamos escobas para colaborar en la limpieza de viviendas afectadas.", "escobas"),
    ("limpieza_calles/casas", "Nos ofrecemos para ayudar con cubos y otros utensilios en la limpieza de refugios.", "cubos"),
    ("limpieza_calles/casas", "Contamos con herramientas para ayudar en la limpieza de un colegio inundado.", "escobas"),
    ("limpieza_calles/casas", "Vamos con cubos y palas para despejar los accesos en zonas anegadas.", "cubos"),

    ("maquinaria", "Disponemos de un tractor para remover escombros y abrir caminos.", "tractor"),
    ("maquinaria", "Podemos llevar una grua para ayudar a mover vehiculos atrapados.", "grua"),
    ("maquinaria", "Ofrecemos un 4x4 para trasladar suministros a zonas de dificil acceso.", "4X4"),
    ("maquinaria", "Llevamos una bomba de agua para ayudar a drenar viviendas inundadas.", "bomba_agua"),
    ("maquinaria", "Tenemos un tractor disponible para despejar carreteras bloqueadas.", "tractor"),

    ("cuidados_medicos", "Soy medico y puedo atender a personas con heridas leves.", "atencion_enfermos"),
    ("cuidados_medicos", "Llevamos medicinas para personas con diabetes que las necesiten.", "medicinas_diabetes"),
    ("cuidados_medicos", "Contamos con medicamentos para quienes padecen problemas del corazon.", "medicinas_corazon"),
    ("cuidados_medicos", "Llevamos medicamentos genericos y material de primeros auxilios.", "medicinas_genericas"),
    ("cuidados_medicos", "Somos un equipo de enfermeros listos para atender a los afectados.", "atencion_enfermos"),

    ("transporte", "Tengo un vehiculo disponible para evacuar personas de zonas afectadas.", "transporte"),
    ("transporte", "Ofrezco transporte para trasladar ancianos a un refugio seguro.", "transporte"),
    ("transporte", "Podemos llevar suministros a barrios aislados en nuestro vehiculo.", "transporte"),
    ("transporte", "Disponemos de espacio para trasladar familias afectadas a otras zonas.", "transporte"),
    ("transporte", "Somos un equipo de voluntarios con furgonetas para ayudar en el traslado de personas.", "transporte")
    ]

    return messages 

def disponibility_options():
    options = ('manana', 'tarde', 'todo_el_dia')
    disponibility = random.choice(options)

    return disponibility

def normalize_names(name):
    return unidecode.unidecode(name)

    