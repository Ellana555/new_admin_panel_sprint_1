from django.contrib import admin
from .models import Genre
from .models import Person
from .models import Filmwork
from .models import GenreFilmwork
from .models import PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    search_fields = ("name",)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    search_fields = ("full_name",)


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline)

    list_display = (
        "title",
        "type",
        "creation_date",
        "rating",
    )
    list_filter = ("type",)
    search_fields = ("title",)
