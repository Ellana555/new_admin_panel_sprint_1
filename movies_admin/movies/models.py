import uuid
from django.db import models
from django.utils.translation import gettext_lazy as _
from django.core.validators import MinValueValidator, MaxValueValidator


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


# Create your models here.
class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_("genre"), max_length=255)
    description = models.TextField(_("description"), blank=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'content"."genre'
        verbose_name = _("genre")
        verbose_name_plural = _("genres")


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_("full name"), max_length=255)

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = 'content"."person'

        verbose_name = _("visible_person")
        verbose_name_plural = _("persons")


# Create your models here.
class Filmwork(UUIDMixin, TimeStampedMixin):
    class Type(models.TextChoices):
        type_movie = "tm", _("tv movie")
        type_tv = "tt", _("tv show")

    title = models.CharField(_("title"), max_length=255)
    description = models.TextField(_("description"), blank=True)
    creation_date = models.DateField(_("creation date"))
    file_path = models.FileField(_('file'), blank=True, null=True, upload_to='movies/')
    rating = models.FloatField(
        _("rating"),
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
    )
    type = models.CharField(_("type"), choices=Type.choices)
    genres = models.ManyToManyField(Genre, through="GenreFilmwork")
    persons = models.ManyToManyField(Person, through="PersonFilmwork")

    def __str__(self):
        return self.title

    class Meta:
        db_table = 'content"."film_work'
        verbose_name = _("movie")
        verbose_name_plural = _("cinema")


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey("Filmwork", on_delete=models.CASCADE)
    genre = models.ForeignKey("Genre", on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.genre

    class Meta:
        db_table = 'content"."genre_film_work'
        verbose_name = _("film genre")
        verbose_name_plural = _("film genres")


class PersonFilmwork(UUIDMixin):
    film_work = models.ForeignKey("Filmwork", on_delete=models.CASCADE)
    person = models.ForeignKey("Person", on_delete=models.CASCADE)
    role = models.TextField(_("role"))
    created = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return str(self.person)

    class Meta:
        db_table = 'content"."person_film_work'
        verbose_name = _("film actor")
        verbose_name_plural = _("film actors")
